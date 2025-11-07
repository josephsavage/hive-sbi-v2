import time
import sys
from datetime import datetime, timezone


from nectar.account import Account
from nectar.utils import formatTimeString
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import AccountsDB, ConfigurationDB


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

                accountStorage.update({
                    "name": acct,
                    "reward_hive": varchar(reward_hive),
                    "reward_hbd": varchar(reward_hbd),
                    "reward_vests": varchar(reward_vests),
                })

            else:
                print(f"No rewards to claim for {account_name}")

        except Exception as e:
            print(f"Error processing {account_name}: {e}")


if __name__ == "__main__":
    main()
