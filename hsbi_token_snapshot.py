
import sys
from datetime import datetime, timezone
from nectar.account import Account
from nectar.utils import formatTimeString
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import AccountsDB, ConfigurationDB
from hivesbi.utils import (
    ensure_timezone_aware,
    estimate_hbd_for_rshares
)
from hivesbi.issue import (
    get_tokenholders, 
    connect_dbs_cached, 
    get_config,,
    get_default_token_issuer
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

    # Fetch tokenholders (defaults to HSBI symbol)
    holders = get_tokenholders()
    db2 = rt.get("db2")
    if db2 is not None:
            issuer = get_default_token_issuer()
            #issue tokens for members with pik > 0
            pending_rows = conn.exec_driver_sql(
                "SELECT member_name, pik FROM tokenholders WHERE pik > 0"
            ).fetchall()

            for member_name, pik in pending_rows:
                print(f"Issuing {pik} HSBI to {member_name}")
                try:
                    tx = issuer.issue(member_name, float(pik))
                    print("Issued:", tx)

                    # Reset pik to 0 after successful issuance
                    conn.exec_driver_sql(
                        "UPDATE tokenholders SET pik = 0 WHERE member_name = %s",
                        (member_name,)
                    )
                except Exception as e:
                    print(f"Failed to issue to {member_name}: {e}")

        
            # Call stored procedure using the raw SQLAlchemy connection
            with db2.engine.begin() as conn:
                print("Upserting tokenholders into DB")
                # Step 1: zero out all balances
                conn.exec_driver_sql("UPDATE tokenholders SET tokens = 0")

                # Step 2: upsert new balances
                for h in holders:
                    conn.exec_driver_sql(
                        """
                        INSERT INTO tokenholders (snapshot_timestamp, member_name, tokens)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            snapshot_timestamp = VALUES(snapshot_timestamp),
                            tokens = VALUES(tokens);

                        """,
                        (datetime.now(timezone.utc), h["account"], h["balance"])
                    )
            
if __name__ == "__main__":
    main()
