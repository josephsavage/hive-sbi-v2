from datetime import datetime

from nectar import Steem
from nectar.utils import addTzInfo
from nectar import Steem



def ensure_timezone_aware(dt):
    """
    Ensures a datetime object has timezone information (UTC).
    If it's already timezone-aware, returns it unchanged.
    If it's timezone-naive or not a datetime, adds UTC timezone.

    Args:
        dt: A datetime object or string representation of a datetime

    Returns:
        datetime: A timezone-aware datetime object (UTC)
    """
    if dt is None:
        return None
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        return addTzInfo(dt)
    return dt



def estimate_rshares_for_hbd(stm: Steem, target_hbd: float, author_share: bool = True) -> int:
    """
    Estimate the rshares required to produce a target HBD payout.
    """
    fund = stm.get_reward_fund("post")
    reward_balance = float(fund["reward_balance"]["amount"])
    recent_claims = int(fund["recent_claims"])

    feed = stm.get_feed_history()
    hive_to_hbd_price = float(feed["current_median_history"]["base"]["amount"]) / float(
        feed["current_median_history"]["quote"]["amount"]

    )

    effective_target = target_hbd * (2 if author_share else 1)
    rshares = (effective_target / hive_to_hbd_price) * (recent_claims / reward_balance)
    return int(rshares)



def estimate_hbd_for_rshares(stm: Steem, rshares: int, author_share: bool = True) -> float:
    """
    Estimate the HBD payout value of a given rshares amount.
    """
    fund = stm.get_reward_fund("post")
    reward_balance = float(fund["reward_balance"]["amount"])
    recent_claims = int(fund["recent_claims"])

    feed = stm.get_feed_history()
    hive_to_hbd_price = float(
        feed["current_median_history"]["base"]["amount"]
    ) / float(feed["current_median_history"]["quote"]["amount"])


    vote_value_hbd = (rshares / recent_claims) * reward_balance * hive_to_hbd_price
    if author_share:
        vote_value_hbd *= 0.5
    return vote_value_hbd
