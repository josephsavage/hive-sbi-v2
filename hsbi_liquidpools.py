import time
from sqlalchemy import text
from datetime import datetime, timezone
from collections import defaultdict
from decimal import Decimal
from hivesbi.storage import ConfigurationDB
from hivesbi.utils import ensure_timezone_aware

from nectarengine.api import Api

from hivesbi.settings import Config, get_runtime

he = Api()


def lp_token_symbol(pair: str) -> str:
    return "LPS" + pair.replace(":", "_")


def get_lp_holders(lp_symbol: str):
    return he.find_all(
        contract_name="marketpools",
        table_name="liquidityPositions",
        query={"tokenPair": lp_symbol},
    )


def get_pool_info(lp_symbol: str):
    return he.find_one(
        contract_name="marketpools",
        table_name="pools",
        query={"tokenPair": lp_symbol},
    )


def extract_hsbidao_amounts(lp_symbol: str):
    """
    Returns a dict: {member_name: Decimal(amount_of_HSBIDAO)}
    for this specific pool.
    """
    holders = get_lp_holders(lp_symbol)
    pool = get_pool_info(lp_symbol)

    if not pool:
        raise RuntimeError(f"Pool not found: {lp_symbol}")

    total_shares = Decimal(pool["totalShares"])
    base_qty = Decimal(pool["baseQuantity"])
    quote_qty = Decimal(pool["quoteQuantity"])

    base_token, quote_token = lp_symbol.split(":")

    # Identify which side is HSBIDAO
    hsbidao_side = None
    if base_token == "HSBIDAO":
        hsbidao_side = ("base", base_qty)
    elif quote_token == "HSBIDAO":
        hsbidao_side = ("quote", quote_qty)
    else:
        # Pool does not contain HSBIDAO at all
        return {}

    side_name, total_hsbidao = hsbidao_side

    per_member = {}

    for row in holders:
        acct = row["account"]
        lp_balance = Decimal(row["shares"])

        if lp_balance == 0:
            continue

        share = lp_balance / total_shares
        user_hsbidao = share * total_hsbidao

        per_member[acct] = user_hsbidao

    return per_member


def aggregate_hsbidao_across_pools(cfg):
    """
    Iterates over all LP symbols in config.LP_SYMBOL
    and aggregates HSBIDAO exposure per member.
    """
    totals = defaultdict(Decimal)

    for lp_symbol in cfg["LP_SYMBOL"]:
        pool_amounts = extract_hsbidao_amounts(lp_symbol)

        for member, amt in pool_amounts.items():
            totals[member] += amt

    return totals


def main():
    rt = get_runtime()
    cfg = rt["cfg"]
    db2 = rt.get("db2")
    
    # Open configuration database via storages
    stor = rt["storages"]
    confStorage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()
    share_cycle_min = conf_setup["share_cycle_min"]

    if db2 is not None:        
        with db2.engine.begin() as conn:
            # get max mana_pct from accounts table
            result = conn.exec_driver_sql(
                "SELECT MAX(mana_pct) AS max_mana_pct FROM accounts"
            ).fetchone()

            max_mana_pct = (
                result.max_mana_pct or 0
            )  # or result.max_mana_pct if using RowMapping
            print("hsbi_liquidpools fetching max VP level: ", max_mana_pct)

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
        print("\nUpdating LP_tokens in tokenholders…")        
        totals = aggregate_hsbidao_across_pools(cfg)

        print("\nHSBIDAO exposure across all configured LP pools:\n")
        for member, amt in sorted(totals.items(), key=lambda x: x[0]):
            print(f"{member}: {amt}")


        
            conn.exec_driver_sql(
                "UPDATE tokenholders SET LP_tokens = 0",
            )

            # Step 2: upsert new balances
            for member_name, lp_amt in totals.items():
                conn.exec_driver_sql(
                    "UPDATE tokenholders SET LP_tokens = %s WHERE member_name = %s",
                    (lp_amt, member_name),
                )
        print("LP_tokens updated.")



if __name__ == "__main__":
    main()
