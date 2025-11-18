import time
import sys
from datetime import datetime, timezone


from nectar.account import Account
from nectar.utils import formatTimeString
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import AccountsDB, ConfigurationDB, KeysDB


def main():
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
    keys_db = stor.get("keys") if stor else KeysDB(db2)

    accounts = account_names or rt.get("accounts", [])

    
    if db2 is not None:
            with db2.engine.begin() as conn:
                # get max mana_pct from accounts table
                result = conn.exec_driver_sql(
                    "SELECT MAX(mana_pct) AS max_mana_pct FROM accounts"
                ).fetchone()

                max_mana_pct = result.max_mana_pct or 0   # or result.max_mana_pct if using RowMapping
                print("hsbi_claim_rewards fetching max VP level: ", max_mana_pct)
    
    mana_threshold = conf_setup.get("mana_pct_target", 0)
    max_mana_threshold = mana_threshold * 1.05            
    # Determine whether a new cycle should run (proper logic from example)
    if (
        max_mana_pct is not None
        and max_mana_pct > max_mana_threshold
    ):        

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
        claimed_count = 0  # counter for accounts with rewards claimed

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
                    claimed_count += 1  # increment counter
                

                else:
                    print(f"No rewards to claim for {account_name}")
                
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


            except Exception as e:
                print(f"Error processing {account_name}: {e}")
            
        # Only run dividends procedure if at least one account claimed rewards
        if claimed_count > 0:
            try:
                with db2.engine.begin() as conn:
                    conn.exec_driver_sql("CALL usp_curation_dividends();")
                    print("usp_curation_dividends executed successfully")
            except Exception as e:
                print(f"Error executing usp_curation_dividends: {e}")


if __name__ == "__main__":
    main()
