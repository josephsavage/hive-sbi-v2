"""Hive Engine token issuance helpers for HSBIDAO."""

from typing import Optional

from nectar.account import Account
from nectarengine.wallet import Wallet as EngineWallet

from hivesbi.settings import Config, get_config, make_hive
from hivesbi.storage import KeysDB

DEFAULT_ISSUER_ACCOUNT = "hivesbi"
DEFAULT_TOKEN_SYMBOL = "HSBIDAO"
DEFAULT_KEY_TYPE = "active"


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
            return self.engine_wallet.transfer(recipient, amount, symbol, memo=memo)

        memo_text = memo or ""
        return self.hive_account.transfer(
            recipient, amount, symbol_upper, memo=memo_text
        )


def get_default_token_issuer() -> "TokenIssuer":
    """Return a cached `TokenIssuer` configured for default HSBIDAO issuance."""

    return TokenIssuer()


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
