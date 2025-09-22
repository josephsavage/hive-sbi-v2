"""
Centralized configuration loader for Hive SBI tools.

Usage:
    from hivesbi.settings import Config, load_config
    cfg = Config.load()            # or load_config()
    db_url = cfg.get("db_url")

Guarantees:
- Reads config.json (path optional). If no path is provided, it searches in:
  1) The explicitly provided path (if given)
  2) Current working directory: ./config.json
  3) Project root directory (repo root): <project_root>/config.json
- Provides dict-like access and attribute-style access to config keys.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

# Helper imports for centralized bootstrap
import dataset
from nectar import Hive
from nectar.nodelist import NodeList

from hivesbi.storage import (
    AccountsDB,
    AuditDB,
    BlacklistDB,
    ConfigurationDB,
    KeysDB,
    MemberDB,
    TransactionMemoDB,
    TransactionOutDB,
    TrxDB,
)


class Config:
    """A simple wrapper around a dict providing convenient access to config values.

    - Dict-like: cfg["key"], "key" in cfg, cfg.get("key", default)
    - Attribute-like: cfg.key
    """

    def __init__(
        self, data: Dict[str, Any], source_path: Optional[Path] = None
    ) -> None:
        self._data = data
        self._source_path = source_path

    # ---- Core API ---------------------------------------------------------
    @classmethod
    def load(cls, path: Optional[os.PathLike[str] | str] = None) -> "Config":
        """Load configuration from config.json.

        Search order:
        1) Explicit path provided via `path`
        2) ./config.json (current working directory)
        3) <project_root>/config.json (directory containing this repo)
        """
        candidate_paths = []

        if path:
            candidate_paths.append(Path(path))
        # CWD
        candidate_paths.append(Path.cwd() / "config.json")
        # Project root (assume two levels up from this file is repo root)
        # hivesbi/settings.py -> project_root/config.json
        project_root = Path(__file__).resolve().parent.parent
        candidate_paths.append(project_root / "config.json")

        for p in candidate_paths:
            if p.is_file():
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                return cls(data, source_path=p)

        raise FileNotFoundError(
            "config.json not found. Looked in: "
            + ", ".join(str(p) for p in candidate_paths)
        )

    # ---- Dict-like methods -----------------------------------------------
    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: object) -> bool:  # type: ignore[override]
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def keys(self) -> Iterable[str]:
        return self._data.keys()

    def items(self) -> Iterable[tuple[str, Any]]:
        return self._data.items()

    def values(self) -> Iterable[Any]:
        return self._data.values()

    # ---- Attribute-style access ------------------------------------------
    def __getattr__(self, name: str) -> Any:
        try:
            return self._data[name]
        except KeyError as e:
            raise AttributeError(name) from e

    # ---- Helpers ----------------------------------------------------------
    @property
    def source_path(self) -> Optional[Path]:
        return self._source_path


# Backwards/utility function for simple usage


def load_config(path: Optional[os.PathLike[str] | str] = None) -> Config:
    return Config.load(path)


# ---- Centralized bootstrap helpers -----------------------------------------
_CFG_CACHE: Optional[Config] = None


def get_config(path: Optional[os.PathLike[str] | str] = None) -> Config:
    """Return a cached Config object, loading config.json only once per process."""
    global _CFG_CACHE
    if _CFG_CACHE is None:
        _CFG_CACHE = Config.load(path)
    return _CFG_CACHE


def connect_dbs(cfg: Config):
    """Open database connections using connectors from config."""
    db = (
        dataset.connect(cfg["databaseConnector"])
        if "databaseConnector" in cfg
        else None
    )
    db2 = (
        dataset.connect(cfg["databaseConnector2"])
        if "databaseConnector2" in cfg
        else None
    )
    db3 = (
        dataset.connect(cfg["databaseConnector3"])
        if "databaseConnector3" in cfg
        else None
    )
    return db, db2, db3


def make_nodes() -> NodeList:
    nodes = NodeList()
    try:
        nodes.update_nodes()
    except Exception:
        # Keep NodeList usable even if update fails; callers still get a default list
        pass
    return nodes


def make_hive(
    cfg: Config,
    keys: Optional[list[str]] = None,
    condenser: bool = False,
    **kwargs,
) -> Hive:
    nodes = make_nodes()
    node_list = (
        nodes.get_nodes(hive=cfg["hive_blockchain"])
        if "hive_blockchain" in cfg
        else None
    )
    if condenser:
        return Hive(keys=keys, node=node_list, use_condenser=True, **kwargs)
    return Hive(keys=keys, node=node_list, **kwargs)


def make_storages(db, db2) -> Dict[str, Any]:
    """Initialize and return common storage objects.

    Keys are:
      - "trx" (TrxDB on db2)
      - "member" (MemberDB on db2)
      - "conf" (ConfigurationDB on db2)
      - "accounts" (AccountsDB on db2)
      - "keys" (KeysDB on db2)
      - "trx_memo" (TransactionMemoDB on db2)
      - "trx_out" (TransactionOutDB on db2)
      - "audit" (AuditDB on db2)
    """
    storages: Dict[str, Any] = {}
    if db2 is not None:
        storages["trx"] = TrxDB(db2)
        storages["member"] = MemberDB(db2)
        storages["conf"] = ConfigurationDB(db2)
        storages["accounts"] = AccountsDB(db2)
        storages["blacklist"] = BlacklistDB(db2)
        storages["keys"] = KeysDB(db2)
        storages["trx_memo"] = TransactionMemoDB(db2)
        storages["trx_out"] = TransactionOutDB(db2)
        storages["audit"] = AuditDB(db2)
    return storages


def get_runtime(path: Optional[os.PathLike[str] | str] = None) -> Dict[str, Any]:
    """Assemble a common runtime package: cfg, dbs, storages, conf, accounts, and a Hive instance."""
    cfg = get_config(path)
    db, db2, db3 = connect_dbs(cfg)
    stor = make_storages(db, db2)
    conf_setup = stor["conf"].get() if "conf" in stor else None
    accounts = stor["accounts"].get() if "accounts" in stor else []
    accounts_data = stor["accounts"].get_data() if "accounts" in stor else {}
    hv = make_hive(cfg)
    return {
        "cfg": cfg,
        "db": db,
        "db2": db2,
        "db3": db3,
        "storages": stor,
        "conf_setup": conf_setup,
        "accounts": accounts,
        "accounts_data": accounts_data,
        "hv": hv,
    }
