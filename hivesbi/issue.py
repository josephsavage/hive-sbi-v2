"""Hive Engine token issuance helpers for HSBIDAO."""

import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from nectar.account import Account
from nectarengine.wallet import Wallet as EngineWallet

from hivesbi.settings import Config, get_config, make_hive
from hivesbi.storage import KeysDB

DEFAULT_ISSUER_ACCOUNT = "hivesbi"
DEFAULT_TOKEN_SYMBOL = "HSBIDAO"
DEFAULT_KEY_TYPE = "active"

# Hive rejects the 6th custom_json an account broadcasts inside one 3-second
# block ("Account hivesbi already submitted 5 custom json operation(s) this
# block"). Every Hive Engine issue/transfer is one custom_json from the same
# issuer account, so the budget is per account per block — it cannot be
# respected by any single loop. hsbi_token_snapshot alone drives three
# independent broadcast loops (pik, Pending Balance Conversion, Management) and
# hivesbi/parse_hist_op adds Unit Conversion, so the pacing lives here, at the
# one place every broadcast passes through.
#
# One second between broadcast starts puts at most 3 ops in a block, leaving
# headroom for whatever another process broadcasts from the same account.
BROADCAST_MIN_INTERVAL = 1.0

# Keyed by account name, measured from the START of the previous broadcast so
# the network round trip counts toward the interval instead of being added on
# top of it.
_last_broadcast_started: dict[str, float] = {}


def throttle_broadcast(account_name: str) -> None:
    """Block until `account_name` may safely broadcast another custom_json."""
    previous = _last_broadcast_started.get(account_name)
    if previous is not None:
        wait = BROADCAST_MIN_INTERVAL - (time.monotonic() - previous)
        if wait > 0:
            time.sleep(wait)
    _last_broadcast_started[account_name] = time.monotonic()


class TokenIssuer:
    """Issue Hive Engine tokens using credentials from configuration and database."""

    def __init__(
        self,
        cfg: Optional[Config] = None,
        account_name: str | None = None,
        key_type: str = "active",
        token_symbol: str | None = None,
    ) -> None:
        self.cfg = cfg or get_config()
        self.account_name = account_name or DEFAULT_ISSUER_ACCOUNT
        self.token_symbol = token_symbol or DEFAULT_TOKEN_SYMBOL
        if not self.account_name:
            raise ValueError("Hive Engine issuer account not configured")
        if not self.token_symbol:
            raise ValueError("Hive Engine token symbol not configured")

        _db1, db2, _db3 = connect_dbs_cached(self.cfg)
        if db2 is None:
            raise ValueError("Database connection for keys (db2) is required")

        keys_storage = KeysDB(db2)
        key_row = keys_storage.get(self.account_name, key_type)
        if key_row is None:
            raise ValueError(
                f"No {key_type} key found for issuer account '{self.account_name}'"
            )
        self.active_key = key_row["wif"].strip()

        self.hive = make_hive(self.cfg, keys=[self.active_key])
        self.engine_wallet = EngineWallet(
            self.account_name, blockchain_instance=self.hive
        )
        self.hive_account = Account(self.account_name, blockchain_instance=self.hive)

    def issue(self, recipient: str, amount: float) -> dict:
        """Issue tokens to the recipient, returning the transaction dict.

        Note: nectarengine Wallet.issue(recipient, amount, symbol) does not support a memo.
        """
        if amount <= 0:
            raise ValueError("Amount must be positive")
        throttle_broadcast(self.account_name)
        return self.engine_wallet.issue(recipient, amount, self.token_symbol)

    def transfer(
        self,
        recipient: str,
        amount: float,
        asset_symbol: str | None = None,
        memo: str | None = None,
        force_engine: bool | None = None,
    ) -> dict:
        """Transfer tokens via Hive Engine or base Hive depending on the symbol.

        If `asset_symbol` (defaulting to the issuer's token symbol) is `"HIVE"` or
        `"HBD"`, the transfer is executed on the base chain using the Nectar account
        API. All other symbols are treated as Hive Engine assets and routed through
        the Hive Engine wallet. Set `force_engine` to override the automatic routing
        decision when needed.
        """

        if amount <= 0:
            raise ValueError("Amount must be positive")
        if not recipient:
            raise ValueError("Recipient account name is required")

        symbol = asset_symbol or self.token_symbol
        if not symbol:
            raise ValueError("Token symbol must be provided for transfers")

        symbol_upper = symbol.upper()
        use_engine = (
            force_engine
            if force_engine is not None
            else symbol_upper not in {"HIVE", "HBD"}
        )

        if use_engine:
            # Base-chain HIVE/HBD transfers are not custom_json and carry no
            # per-block limit, so only the engine path is throttled.
            throttle_broadcast(self.account_name)
            return self.engine_wallet.transfer(recipient, amount, symbol, memo=memo)

        memo_text = memo or ""
        return self.hive_account.transfer(
            recipient, amount, symbol_upper, memo=memo_text
        )


_default_issuer: Optional["TokenIssuer"] = None


def get_default_token_issuer() -> "TokenIssuer":
    """Return a cached `TokenIssuer` configured for default HSBIDAO issuance.

    Cached because hivesbi/parse_hist_op calls this once per Unit Conversion,
    and rebuilding the issuer each time re-reads the active key, reconnects Hive
    and refetches the account. Note this is not what makes broadcast pacing work:
    `_last_broadcast_started` is module state keyed by account name, so throttling
    survives a rebuilt issuer either way.
    """

    global _default_issuer
    if _default_issuer is None:
        _default_issuer = TokenIssuer()
    return _default_issuer


def issue_default_tokens(recipient: str, amount: float) -> dict:
    """Issue default HSBIDAO tokens using the cached issuer."""

    issuer = get_default_token_issuer()
    return issuer.issue(recipient, amount)


_config_cache: Optional[Config] = None
_db_cache: tuple = (None, None, None)


def connect_dbs_cached(cfg: Config):
    global _config_cache, _db_cache
    if _config_cache is cfg and all(v is not None for v in _db_cache):
        return _db_cache
    from hivesbi.settings import connect_dbs

    _config_cache = cfg
    _db_cache = connect_dbs(cfg)
    return _db_cache

from nectarengine.api import Api

def get_tokenholders(symbol: str | None = None, limit: int = 1000, offset: int = 0) -> list[dict]:
    """
    Return a list of account/balance dicts for the given Hive Engine token symbol.
    Defaults to the configured DEFAULT_TOKEN_SYMBOL if none is provided.

    Each dict has at least:
      - account: Hive account name
      - balance: string balance amount
    """
    token_symbol = (symbol or DEFAULT_TOKEN_SYMBOL).upper()
    api = Api()

    holders: list[dict] = []
    while True:
        # Try the most explicit, keyword-argument call first (current code).
        # If the installed nectarengine.Api.find has a different signature we'll fall
        # back to positional forms.
        page = None
        try:
            page = api.find(
                contract="tokens",
                table="balances",
                query={"symbol": token_symbol},
                limit=limit,
                offset=offset,
            )
        except TypeError:
            # Fallback: try common positional-signature variants.
            # Variant 1: api.find(contract, table, query, limit, offset)
            try:
                page = api.find("tokens", "balances", {"symbol": token_symbol}, limit, offset)
            except TypeError:
                # Variant 2: api.find(table, query, limit, offset) (no contract param)
                try:
                    page = api.find("balances", {"symbol": token_symbol}, limit, offset)
                except Exception as e:
                    # If all attempts fail, re-raise the original TypeError to show the root cause.
                    raise

        if not page:
            break
        # Only keep account and balance fields
        for row in page:
            holders.append({
                "account": row["account"],
                "balance": row["balance"],
            })
        offset += limit

    return holders


def fetch_issues_to_recipient(
    recipient: str,
    start,
    end,
    symbol: str | None = None,
    issuer_account: str | None = None,
    limit: int = 1000,
):
    """Hive Engine `tokens_issue` ops received by one account in a time window.

    Answers "did this specific issuance reach the chain?" in a single request,
    where walking the issuer's own custom_json history costs tens of thousands of
    ops. Because the query is scoped to one recipient, one symbol and a
    35-minute window, its cost does not grow with how long a row has been stuck —
    which is what makes an old PENDING row resolvable at all.

    `start` and `end` are timezone-aware datetimes. Returns a list of dicts with
    trx_id / recipient / quantity / timestamp, or None when the lookup could not
    be trusted (transport error, or a response that is not the expected list).
    None means "unknown" and callers must leave the row PENDING; an empty list
    means "provably nothing was issued in this window".
    """
    token_symbol = (symbol or DEFAULT_TOKEN_SYMBOL).upper()
    issuer_account = issuer_account or DEFAULT_ISSUER_ACCOUNT
    api = Api()

    params = {
        "account": recipient,
        "symbol": token_symbol,
        "limit": limit,
        "offset": 0,
        "timestampStart": int(start.timestamp()),
        "timestampEnd": int(end.timestamp()),
    }
    try:
        response = httpx.get(
            f"{api.history_url}accountHistory", params=params, timeout=30
        )
        if response.status_code != 200:
            return None
        payload = response.json()
    except Exception as exc:
        print(f"Hive Engine history lookup failed for {recipient}: {exc}")
        return None

    if not isinstance(payload, list):
        return None

    issues = []
    for row in payload:
        if not isinstance(row, dict):
            return None
        if row.get("operation") != "tokens_issue":
            continue
        if row.get("to") != recipient:
            continue
        if row.get("issuer") != issuer_account:
            continue
        trx_id = row.get("transactionId")
        quantity = row.get("quantity")
        timestamp = row.get("timestamp")
        if not trx_id or quantity is None or timestamp is None:
            return None
        issues.append(
            {
                "trx_id": str(trx_id),
                "recipient": recipient,
                "quantity": quantity,
                "timestamp": datetime.fromtimestamp(int(timestamp), timezone.utc),
            }
        )
    return issues
