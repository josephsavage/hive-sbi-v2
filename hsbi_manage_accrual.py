#!/usr/bin/env python
import json
import os
import sys
from datetime import datetime, timezone

import dataset
from nectar import Hive
from nectar.account import Account
from nectar.nodelist import NodeList

from hivesbi.storage import AccountsDB, ConfigurationDB
from hivesbi.utils import (
    ensure_timezone_aware,
    estimate_hbd_for_rshares,
    estimate_rshares_for_hbd,
)

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

    # Determine whether a new cycle should run (proper logic from example)
    elapsed_min = (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60
    print(f"sbi_manage_accrual: last_cycle is {last_cycle} ({elapsed_min:.2f} min ago)")
    if (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds()
        > 60 * share_cycle_min
    ):
#        try:
#            # Get dbconnector3 from config.json
#            databaseConnector3 = config_data["databaseConnector3"]
#
#            # Connect to dbconnector3
#            db3 = dataset.connect(databaseConnector3)
#
#            # Get the raw SQLAlchemy connection so we can call the stored procedure
#            with db3.engine.begin() as conn:
#                print("Calling stored procedure: sbi_reporting.python_call_usp_list()")
#                result = conn.exec_driver_sql(
#                    "CALL sbi_reporting.python_call_usp_list()"
#                )
#
#                # Iterate over any returned rows and print them
#                for row in result:
#                    # row can be a tuple or Row object depending on driver
#                    print("LOG:", *row)
#
#        except Exception as e:
#            print(f"Error calling stored procedure: {e}")

        # Build Hive instance and collect mana for each account
        nodes = NodeList()
        nodes.update_nodes()
        node_list = nodes.get_nodes(hive=hive_blockchain)
        hv = Hive(node=node_list, num_retries=5, call_num_retries=3, timeout=15)

        rshares_needed = estimate_rshares_for_hbd(hv, 0.021)
        print(
            f"Target threshold: {rshares_needed} rshares (≈ {estimate_hbd_for_rshares(hv, rshares_needed):.5f} HBD)"
        )

        total_current_mana = 0
        total_max_mana = 0
        accounts_processed = 0
        for acc in account_names:
            try:
                mana = Account(acc, blockchain_instance=hv).get_manabar()
                total_current_mana += mana.get("current_mana", 0)
                total_max_mana += mana.get("max_mana", 0)
                accounts_processed += 1
            except Exception as e:
                print(f"Could not fetch mana for {acc}: {e}")

        if total_max_mana == 0:
            print("Unable to retrieve mana information for any account. Exiting.")
            sys.exit(1)

        overall_mana_pct = (total_current_mana / total_max_mana) * 100
        print(
            f"Overall mana across {accounts_processed} accounts: {overall_mana_pct:.2f}%"
        )

        # Adjust accrual rates based on 50% threshold
        factor = 1.025 if overall_mana_pct > 50 else 0.99
        rshares_per_cycle *= factor
        del_rshares_per_cycle *= factor
        calc_min_threshold = rshares_needed

        # Persist updated values and reset last_cycle
        confStorage.update(
            {
                "rshares_per_cycle": rshares_per_cycle,
                "del_rshares_per_cycle": del_rshares_per_cycle,
                "calc_min_threshold": calc_min_threshold,
                # "last_cycle": datetime.now(timezone.utc), # TODO: enable this if it's needed
            }
        )
        print(f"Updated rshares_per_cycle to {rshares_per_cycle:.6f}")
        print(f"Updated del_rshares_per_cycle to {del_rshares_per_cycle:.6f}")
    else:
        print("Not time for a new cycle yet. Exiting.")
