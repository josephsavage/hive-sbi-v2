#!/usr/bin/env python
import json
import os
import sys
from datetime import datetime, timezone

import dataset
from nectar import Steem
from nectar.account import Account
from nectar.nodelist import NodeList

from steembi.storage import AccountsDB, ConfigurationDB
from steembi.utils import ensure_timezone_aware

if __name__ == "__main__":
    # Load configuration from config.json (same as other SBI scripts)
    config_file = "config.json"
    if not os.path.isfile(config_file):
        print("config.json is missing!")
        sys.exit(1)
    with open(config_file) as f:
        config_data = json.load(f)

    databaseConnector2 = config_data["databaseConnector2"]
    hive_blockchain = config_data["hive_blockchain"]

    # Open configuration database
    db2 = dataset.connect(databaseConnector2)
    confStorage = ConfigurationDB(db2)
    conf_setup = confStorage.get()

    # Fetch account list from the accounts table instead of config.json
    accountStorage = AccountsDB(db2)
    account_names = accountStorage.get()

    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    del_rshares_per_cycle = conf_setup["del_rshares_per_cycle"]

    # Determine whether a new cycle should run (same logic as sbi_reset_rshares)
    elapsed_min = (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60
    print(f"sbi_manage_accrual: last_cycle is {last_cycle} ({elapsed_min:.2f} min ago)")

    if elapsed_min < share_cycle_min:
        print(
            f"Not time for a new cycle yet (share_cycle_min={share_cycle_min}). Exiting."
        )
        sys.exit(0)

    # Build Steem instance and collect mana for each account
    nodes = NodeList()
    nodes.update_nodes()
    node_list = nodes.get_nodes(hive=hive_blockchain)
    stm = Steem(node=node_list, num_retries=5, call_num_retries=3, timeout=15)

    mana_pcts = []
    for acc in account_names:
        try:
            mana = Account(acc, steem_instance=stm).get_manabar()
            mana_pcts.append(mana.get("current_mana_pct", 0))
        except Exception as e:
            print(f"Could not fetch mana for {acc}: {e}")

    if not mana_pcts:
        print("Unable to retrieve mana for any account. Exiting.")
        sys.exit(1)

    avg_mana_pct = sum(mana_pcts) / len(mana_pcts)
    print(f"Average mana across {len(mana_pcts)} accounts: {avg_mana_pct:.2f}%")

    # Adjust accrual rates based on 50% threshold
    factor = 1.01 if avg_mana_pct > 50 else 0.99
    rshares_per_cycle *= factor
    del_rshares_per_cycle *= factor

    # Persist updated values and reset last_cycle
    confStorage.update(
        {
            "rshares_per_cycle": rshares_per_cycle,
            "del_rshares_per_cycle": del_rshares_per_cycle,
            # "last_cycle": datetime.now(timezone.utc), # TODO: enable this if it's needed
        }
    )

    print(f"Updated rshares_per_cycle to {rshares_per_cycle:.6f}")
    print(f"Updated del_rshares_per_cycle to {del_rshares_per_cycle:.6f}")
