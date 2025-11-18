
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
    get_config,
    get_default_token_issuer
)

def main():
    rt = get_runtime()
    cfg = rt["cfg"]

    # Open configuration database via storages
    stor = rt["storages"]
    confStorage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()
    share_cycle_min = conf_setup["share_cycle_min"]

    # Fetch tokenholders (defaults to HSBIDAO symbol)
    holders = get_tokenholders()
    db2 = rt.get("db2")
    if db2 is not None:
            with db2.engine.begin() as conn:
                issuer = get_default_token_issuer()
                #issue tokens for members with pik > 0
                pending_rows = conn.exec_driver_sql(
                    "SELECT member_name, pik FROM tokenholders WHERE pik > 0"
                ).fetchall()

                for member_name, pik in pending_rows:
                    print(f"Issuing {pik} HSBI to {member_name}")
                    try:
                        tx = issuer.issue(member_name, float(pik))
                        trx_id = tx.get("trx_id")  # extract the string

                        print("Issued:", tx)

                        # Reset pik to 0 after successful issuance
                        conn.exec_driver_sql(
                            "UPDATE tokenholders SET pik = 0 WHERE member_name = %s",
                            (member_name,)
                        )
                        # Log success
                        conn.exec_driver_sql(
                            """
                            INSERT INTO token_issuance_log (trx_id, recipient, units, status, error_message, rationale)
                            VALUES (%s, %s, %s, %s, NULL, %s)
                            """,
                            (trx_id, member_name, pik, "SUCCESS", "pik"),
                        )

                    except Exception as e:
                        print(f"Failed to issue to {member_name}: {e}")
                        # Log failure
                        conn.exec_driver_sql(
                            """
                            INSERT INTO token_issuance_log (trx_id, recipient, units, status, error_message, rationale)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            ("N/A", member_name, pik, "FAILURE", str(e), "FAILURE"),
                        )


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
