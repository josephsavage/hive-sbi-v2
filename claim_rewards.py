import time
from nectar.account import Account

from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import AccountsDB, KeysDB


def main():
    # Load a standard runtime bundle (cfg, db connections, storages, hv, accounts)
    rt = get_runtime()
    cfg = rt["cfg"]
    db = rt["db"]
    db2 = rt["db2"]

    # Prefer storages from the runtime if available, otherwise instantiate
    storages = rt.get("storages", {})
    accounts_db = storages.get("accounts") if storages else AccountsDB(db2)
    keys_db = storages.get("keys") if storages else KeysDB(db2)

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
    hv = make_hive(cfg, keys=posting_keys)

    # Iterate accounts and claim rewards when present
    for account_name in accounts:
        try:
            acct = Account(account_name, blockchain_instance=hv)

            # Keep underlying attribute names from the account object but rename
            # local variables to repository conventions (hive/hbd/vesting)
            reward_hive = acct.reward_hive_balance
            reward_hbd = acct.reward_hbd_balance
            reward_vesting = acct.reward_vesting_balance

            print(
                f"{account_name}: reward_hive={reward_hive}, reward_hbd={reward_hbd}, reward_vesting={reward_vesting}"
            )

            rewards = [reward_hive, reward_hbd, reward_vesting]
            # reward objects are expected to have an 'amount' attribute convertible to float
            has_rewards = any(float(r.amount) > 0 for r in rewards)

            if has_rewards:
                print(f"Claiming rewards for {account_name}")
                acct.claim_rewards()
                time.sleep(3)  # avoid rate limits
            else:
                print(f"No rewards to claim for {account_name}")
        except Exception as e:
            print(f"Error processing {account_name}: {e}")


if __name__ == "__main__":
    main()