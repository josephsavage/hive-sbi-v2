import time
from datetime import datetime, timezone
from hivesbi.settings import get_runtime
from hivesbi.storage import ConfigurationDB
from hivesbi.utils import ensure_timezone_aware
from hivesbi.issue import (
    get_tokenholders,
    get_default_token_issuer,
)


BATCH_SLEEP_TIME = 3


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
            # get max mana_pct from accounts table
            result = conn.exec_driver_sql(
                "SELECT MAX(mana_pct) AS max_mana_pct FROM accounts"
            ).fetchone()

            max_mana_pct = (
                result.max_mana_pct or 0
            )  # or result.max_mana_pct if using RowMapping
            print("hsbi_token_snapshot fetching max VP level: ", max_mana_pct)

    mana_pct_target = conf_setup.get("mana_pct_target", 0)
    mana_threshold = conf_setup.get("mana_threshold", 0)
    max_mana_threshold = mana_threshold * mana_pct_target
    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])

    # Determine whether a new cycle should run (proper logic from example)
    if (max_mana_pct is not None and max_mana_pct > max_mana_threshold) or (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds()
        > 60 * share_cycle_min
    ):
        # curation PIK token issuance
        with db2.engine.begin() as conn:
            issuer = get_default_token_issuer()
            # issue tokens for members with pik > 0
            pending_rows = conn.exec_driver_sql(
                "SELECT member_name, pik FROM tokenholders WHERE pik > 0"
            ).fetchall()

            for i, (member_name, pik) in enumerate(pending_rows):
                if i > 0 and i % 5 == 0:
                    print(f"Sleeping for {BATCH_SLEEP_TIME} seconds...")
                    time.sleep(BATCH_SLEEP_TIME)
                print(f"Issuing {pik} HSBIDAO to {member_name}")
                try:
                    # 1. Blockchain side-effect (cannot be rolled back)

                    tx = issuer.issue(member_name, float(pik))
                    trx_id = tx.get("trx_id")  # extract the string

                    print("Issued:", tx)

                    # 2. Now open a DB transaction for the state change + audit log
                    with db2.engine.begin() as conn:
                        conn.execute(
                            text("UPDATE tokenholders SET pik = 0 WHERE member_name = :m"),
                            {"m": member_name},
                        )

                        conn.execute(
                            text("""
                                INSERT INTO token_issuance_log
                                    (trx_id, recipient, units, status, error_message, rationale)
                                VALUES
                                    (:trx, :recipient, :units, 'SUCCESS', NULL, 'pik')
                            """),
                            {"trx": trx_id, "recipient": member_name, "units": pik},
                        )

                except Exception as e:
                    print(f"Failed to issue to {member_name}: {e}")

                    # Log failure in its own transaction
                    with db2.engine.begin() as conn:
                        conn.execute(
                            text("""
                                INSERT INTO token_issuance_log
                                    (trx_id, recipient, units, status, error_message, rationale)
                                VALUES
                                    ('N/A', :recipient, :units, 'FAILURE', :err, 'FAILURE')
                            """),
                            {"recipient": member_name, "units": pik, "err": str(e)},
                        )


        # Pending Balance Conversion logic here
        with db2.engine.begin() as conn:
            issuer = get_default_token_issuer()
            # issue tokens for members with abc_pik > 0
            pending_rows = conn.exec_driver_sql(
                "SELECT member_name, abc_pik FROM tokenholders WHERE abc_pik > 0"
            ).fetchall()

            for i, (member_name, abc_pik) in enumerate(pending_rows):
                if i > 0 and i % 5 == 0:
                    print(f"Sleeping for {BATCH_SLEEP_TIME} seconds...")
                    time.sleep(BATCH_SLEEP_TIME)
                print(f"Issuing {abc_pik} HSBIDAO to {member_name}")
                try:
                    tx = issuer.issue(member_name, float(abc_pik))
                    trx_id = tx.get("trx_id")  # extract the string

                    print("Issued:", tx)

                    # Reset abc_pik to 0 after successful issuance
                    conn.exec_driver_sql(
                        "UPDATE tokenholders SET abc_pik = 0 WHERE member_name = %s",
                        (member_name,),
                    )
                    # Log success
                    conn.exec_driver_sql(
                        """
                            INSERT INTO token_issuance_log (trx_id, recipient, units, status, error_message, rationale)
                            VALUES (%s, %s, %s, %s, NULL, %s)
                            """,
                        (
                            trx_id,
                            member_name,
                            abc_pik,
                            "SUCCESS",
                            "Pending Balance Conversion",
                        ),
                    )

                except Exception as e:
                    print(f"Failed to issue to {member_name}: {e}")
                    # Log failure
                    conn.exec_driver_sql(
                        """
                            INSERT INTO token_issuance_log (trx_id, recipient, units, status, error_message, rationale)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                        ("N/A", member_name, abc_pik, "FAILURE", str(e), "FAILURE"),
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
                    (datetime.now(timezone.utc), h["account"], h["balance"]),
                )


if __name__ == "__main__":
    main()
