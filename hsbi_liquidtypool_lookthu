from decimal import Decimal
from collections import defaultdict

from nectar.engine import HiveEngine
from hivesbi.settings import config

he = HiveEngine()

def lp_token_symbol(pair: str) -> str:
    return "LPS" + pair.replace(":", "_")

def get_lp_holders(lp_symbol: str):
    lp_token = lp_token_symbol(lp_symbol)
    return he.find(
        contract="tokens",
        table="balances",
        query={"symbol": lp_token},
        limit=5000,
    )

def get_pool_info(lp_symbol: str):
    return he.find_one(
        contract="marketpools",
        table="pools",
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
        lp_balance = Decimal(row["balance"])

        if lp_balance == 0:
            continue

        share = lp_balance / total_shares
        user_hsbidao = share * total_hsbidao

        per_member[acct] = user_hsbidao

    return per_member


def aggregate_hsbidao_across_pools():
    """
    Iterates over all LP symbols in config.LP_SYMBOL
    and aggregates HSBIDAO exposure per member.
    """
    totals = defaultdict(Decimal)

    for lp_symbol in config.LP_SYMBOL:
        pool_amounts = extract_hsbidao_amounts(lp_symbol)

        for member, amt in pool_amounts.items():
            totals[member] += amt

    return totals


if __name__ == "__main__":
    totals = aggregate_hsbidao_across_pools()

    print("\nHSBIDAO exposure across all configured LP pools:\n")
    for member, amt in sorted(totals.items(), key=lambda x: x[0]):
        print(f"{member}: {amt}")
