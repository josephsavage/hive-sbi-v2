
import sys
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

def main():
    rt = get_runtime()
    cfg = rt["cfg"]

    # Open configuration database via storages
    stor = rt["storages"]
    confStorage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()
    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]

    # Determine whether a new cycle should run (proper logic from example)
    elapsed_min = (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60
    print(
        f"hsbi_token_snapshot: last_cycle is {last_cycle} ({elapsed_min:.2f} min ago)"
    )
    if (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds()
        > 60 * share_cycle_min
    ):        
        # Fetch tokenholders (defaults to HSBI symbol)
        holders = get_tokenholders()

        insert_sql = """
            INSERT INTO tokenholders (snapshot_timestamp, member_name, tokens)
            VALUES (%s, %s, %s, %s)
        """
        log.info("Inserted %s tokenholders", count)
    else:
            print("hsbi_manage_accrual: Not time for a new cycle yet. Exiting.")
            
if __name__ == "__main__":
    main()
