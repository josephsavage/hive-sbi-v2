import sys
import logging
from datetime import datetime, timezone
from nectar.account import Account
from nectar.utils import formatTimeString
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import AccountsDB, ConfigurationDB
from hivesbi.utils import (
    ensure_timezone_aware,
    estimate_hbd_for_rshares,
)
from hivesbi.issue import (
    get_tokenholders,
    connect_dbs_cached,
    get_config,
)

log = logging.getLogger(__name__)

def main():
    rt = get_runtime()
    cfg = rt.get("cfg", {})
    stor = rt.get("storages", {})
    confStorage: ConfigurationDB = stor.get("conf")
    conf_setup = confStorage.get() if confStorage is not None else {}
    last_cycle = conf_setup.get("last_cycle")
    if last_cycle is not None:
        last_cycle = ensure_timezone_aware(last_cycle)

    share_cycle_min = cfg.get("share_cycle_min", 60)

    now = datetime.now(timezone.utc)
    elapsed_min = None
    if last_cycle is not None:
        elapsed_min = (now - last_cycle).total_seconds() / 60.0

    log.info("hsbi_token_snapshot: last_cycle is %s (%s min ago)", last_cycle, f"{elapsed_min:.2f}" if elapsed_min is not None else "N/A")

    # If first run (no last_cycle) or enough minutes elapsed, create a new snapshot
    should_run = last_cycle is None or (elapsed_min is not None and elapsed_min > share_cycle_min)

    if not should_run:
        print("hsbi_token_snapshot: Not time for a new cycle yet. Exiting.")
        return

    # Connect to DBs (uses workspace helper)
    db2 = None
    cur = None
    try:
        db2, cur = connect_dbs_cached()  # expects (connection, cursor)-style return
    except Exception as exc:
        log.exception("Failed to connect to DBs: %s", exc)
        raise

    try:
        # Determine next batch_id
        cur.execute("SELECT COALESCE(MAX(batch_id), 0) + 1 FROM tokenholders")
        batch_id = cur.fetchone()[0]

        # Fetch tokenholders (defaults to HSBI symbol)
        holders = get_tokenholders()

        insert_sql = """
            INSERT INTO tokenholders (snapshot_timestamp, member_name, tokens, batch_id)
            VALUES (%s, %s, %s, %s)
        """

        count = 0
        ts = now
        for h in holders:
            cur.execute(insert_sql, (ts, h["account"], h["balance"], batch_id))
            count += 1

        db2.commit()
        log.info("Inserted %s tokenholders (batch_id=%s)", count, batch_id)

        # Update last_cycle in configuration storage if available
        if confStorage is not None:
            conf_setup["last_cycle"] = ts
            confStorage.set(conf_setup)

    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if db2 is not None:
                db2.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
