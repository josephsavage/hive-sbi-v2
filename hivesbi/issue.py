"""Hive Engine token issuance helpers for HSBI."""

from functools import lru_cache
from typing import Optional

from nectarengine.wallet import Wallet

from hivesbi.settings import Config, get_config, make_hive
from hivesbi.storage import KeysDB

DEFAULT_ISSUER_ACCOUNT = "sbi-tokens"
DEFAULT_TOKEN_SYMBOL = "HSBI"
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
        self.wallet = Wallet(self.account_name, blockchain_instance=self.hive)

    def issue(self, recipient: str, amount: float, memo: str | None = None) -> dict:
        """Issue tokens to the recipient, returning the transaction dict."""
        if amount <= 0:
            raise ValueError("Amount must be positive")
        return self.wallet.issue(recipient, amount, self.token_symbol, memo=memo)


@lru_cache(maxsize=1)
def get_default_token_issuer() -> "TokenIssuer":
    """Return a cached `TokenIssuer` configured for default HSBI issuance."""

    return TokenIssuer()


def issue_default_tokens(
    recipient: str, amount: float, memo: str | None = None
) -> dict:
    """Issue default HSBI tokens using the cached issuer."""

    issuer = get_default_token_issuer()
    return issuer.issue(recipient, amount, memo=memo)


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

