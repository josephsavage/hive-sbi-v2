from nectar import Api

api = Api()

def estimate_rshares_for_hbd(target_hbd: float, author_share: bool = True) -> int:
    fund = api.get_reward_fund("post")
    reward_balance = float(fund["reward_balance"].split()[0])
    recent_claims = int(fund["recent_claims"])

    feed = api.get_feed_history()
    hive_to_hbd_price = (
        float(feed["current_median_history"]["base"].split()[0]) /
        float(feed["current_median_history"]["quote"].split()[0])
    )

    effective_target = target_hbd * (2 if author_share else 1)
    rshares = (effective_target / hive_to_hbd_price) * (recent_claims / reward_balance)
    return int(rshares)


def estimate_hbd_for_rshares(rshares: int, author_share: bool = True) -> float:
    fund = api.get_reward_fund("post")
    reward_balance = float(fund["reward_balance"].split()[0])
    recent_claims = int(fund["recent_claims"])

    feed = api.get_feed_history()
    hive_to_hbd_price = (
        float(feed["current_median_history"]["base"].split()[0]) /
        float(feed["current_median_history"]["quote"].split()[0])
    )

    vote_value_hbd = (rshares / recent_claims) * reward_balance * hive_to_hbd_price
    if author_share:
        vote_value_hbd *= 0.5
    return vote_value_hbd
