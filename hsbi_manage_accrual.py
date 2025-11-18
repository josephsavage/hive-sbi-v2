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


def run():
    rt = get_runtime()
    cfg = rt["cfg"]

    # Open configuration database via storages
    stor = rt["storages"]
    confStorage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()
    
    # Fetch account list from the accounts table
    accountStorage: AccountsDB = stor["accounts"]
    account_names = accountStorage.get()

    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    del_rshares_per_cycle = conf_setup["del_rshares_per_cycle"]
    mana_pct_target = conf_setup["mana_pct_target"]
    
    # Determine whether a new cycle should run (proper logic from example)
    elapsed_min = (datetime.now(timezone.utc) - ).total_seconds() / 60
    print(
        f"hsbi_manage_accrual:  is {} ({elapsed_min:.2f} min ago)"
    )
    if (
        max_mana_pct is not None
        and max_mana_pct > max_mana_threshold
    ):        
        # Build Hive instance and collect mana for each account
        hv = make_hive(cfg, num_retries=5, call_num_retries=3, timeout=15)
        rshares_needed = hv.hbd_to_rshares(0.021)
        print(
            f"hsbi_manage_accrual: Target threshold: {rshares_needed} rshares (≈ {estimate_hbd_for_rshares(hv, rshares_needed):.5f} HBD)"
        )
        total_current_mana = 0
        total_max_mana = 0
        accounts_processed = 0
        for acc in account_names:
            try:
                mana = Account(acc, blockchain_instance=hv).get_manabar()
                current_mana = mana.get("current_mana", 0)
                total_current_mana += mana.get("current_mana", 0)
                max_mana = mana.get("max_mana", 0)
                total_max_mana += mana.get("max_mana", 0)
                mana_pct = (current_mana / max_mana * 100) if max_mana else 0
                accounts_processed += 1

                accountStorage.update({
                    "name": acc,
                    "current_mana": int(current_mana),
                    "max_mana": int(max_mana),
                    "mana_pct": float(mana_pct),
                    # Store UTC without tzinfo to match TIMESTAMP/DATETIME
                    "last_checked": datetime.now(timezone.utc),
                })

            except Exception as e:
                print(f"hsbi_manage_accrual: Could not fetch mana for {acc}: {e}")

        if total_max_mana == 0:
            print(
                "hsbi_manage_accrual: Unable to retrieve mana information for any account. Exiting."
            )
            sys.exit(1)

        overall_mana_pct = (total_current_mana / total_max_mana) * 100
        print(
            f"hsbi_manage_accrual: Overall mana across {accounts_processed} accounts: {overall_mana_pct:.2f}%"
        )

        # Adjust accrual rates based on 50% threshold
        factor = 1.025 if overall_mana_pct > mana_pct_target else 0.99
        rshares_per_cycle *= factor
        del_rshares_per_cycle *= factor
        minimum_vote_threshold = rshares_needed

        # Persist updated values and reset last_cycle
        confStorage.update(
            {
                "rshares_per_cycle": rshares_per_cycle,
                "del_rshares_per_cycle": del_rshares_per_cycle,
                "minimum_vote_threshold": minimum_vote_threshold,
                # "last_cycle": datetime.now(timezone.utc), # TODO: enable this if it's needed
            }
        )
        print(
            f"hsbi_manage_accrual: Updated rshares_per_cycle to {rshares_per_cycle:.6f}"
        )
        print(
            f"hsbi_manage_accrual: Updated del_rshares_per_cycle to {del_rshares_per_cycle:.6f}"
        )
        
        if cfg.get("build_reporting", False):
            try:
                # Example: use third DB connector directly from the runtime
                # Note: rt is provided by get_runtime() above
                db3 = rt.get("db3")
                if db3 is not None:
                    # Call stored procedure using the raw SQLAlchemy connection
                    with db3.engine.begin() as conn:
                        print(
                            "Calling stored procedure: sbi_reporting.python_call_usp_list()"
                        )
                        result = conn.exec_driver_sql(
                            "CALL sbi_reporting.python_call_usp_list()"
                        )
                        # Iterate over any returned rows and print them
                        for row in result:
                            # row can be a tuple or Row object depending on driver
                            print("LOG:", *row)

            except Exception as e:
                print(f"Error calling stored procedure: {e}")
        else:
            print(
                "hsbi_manage_accrual: build_reporting is false; skipping reporting procedure call"
            )

    else:
        print("hsbi_manage_accrual: Not time for a new cycle yet. Exiting.")


if __name__ == "__main__":
    run()
