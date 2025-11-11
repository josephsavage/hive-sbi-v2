import time
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

def snapshot():
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

        
if __name__ == "__snapshot__":
    snapshot()
    
def claim():
    # Load a standard runtime bundle (cfg, db connections, storages, hv, accounts)
    rt = get_runtime()
    cfg = rt["cfg"]
    db = rt["db"]
    db2 = rt["db2"]

    # Prefer storages from the runtime if available, otherwise instantiate
    stor = rt.get("storages", {})
    confStorage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()
    
    # Fetch account list from the accounts table
    accountStorage: AccountsDB = stor["accounts"]
    account_names = accountStorage.get()
    accounts_db = stor.get("accounts") if stor else AccountsDB(db2)
    keys_db = stor.get("keys") if stor else KeysDB(db2)

    accounts = rt.get("accounts", [])

    # Gather posting keys for each account (used to sign claim ops)
    posting_keys = []
    for account_name in accounts:
        k = keys_db.get(account_name, "posting")
        if k and k.get("key_type") == "posting":
            posting_keys.append(k.get("wif", "").strip())
        else:
            print(f"No posting key found for account {account_name}")

    # Build a Hive instance with collected posting keys
    hv = make_hive(cfg, keys=posting_keys, num_retries=5, call_num_retries=3, timeout=15)

    # Iterate accounts and claim rewards when present
    for account_name in accounts:
        try:
            acct = Account(account_name, blockchain_instance=hv)

            reward_hive = acct["reward_hive_balance"]      # Amount object
            reward_hbd = acct["reward_hbd_balance"]        # Amount object
            reward_vests = acct["reward_vesting_balance"]  # Amount object

            print(f"{account_name}: reward_hive={reward_hive}, reward_hbd={reward_hbd}, reward_vests={reward_vests}")

            has_rewards = any(float(r.amount) > 0 for r in [reward_hive, reward_hbd, reward_vests])

            if has_rewards:
                print(f"Claiming rewards for {account_name}")
                acct.claim_reward_balance(
                    reward_hive,
                    reward_hbd,
                    reward_vests
                )
                time.sleep(3)
                
                print("Updating accountStorage with:", {
                    "name": account_name,
                    "reward_hive": str(reward_hive),
                    "reward_hbd": str(reward_hbd),
                    "reward_vests": str(reward_vests),
                })
                
                accountStorage.update({
                    "name": account_name,
                    "reward_hive": str(reward_hive),
                    "reward_hbd": str(reward_hbd),
                    "reward_vests": str(reward_vests),
                })

                # Immediately run the curation dividends procedure
                try:
                    with db2.engine.begin() as conn:
                        conn.exec_driver_sql("CALL usp_curation_dividends();")
                        print("usp_curation_dividends executed successfully")
                except Exception as e:
                    print(f"Error executing usp_curation_dividends: {e}")

            else:
                print(f"No rewards to claim for {account_name}")

        except Exception as e:
            print(f"Error processing {account_name}: {e}")


if __name__ == "__claim__":
    claim()
