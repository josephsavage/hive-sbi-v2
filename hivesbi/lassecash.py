"""LasseCash companion votes on SBI members' `lassecash` posts.

LasseCash rewards posts from a contract on Magi (formerly VSC). A Hive vote
never reaches that contract: only a `vsc.call` custom_json with action `vote`
counts. hsbi_lassecash_vote casts that call from one account that holds
L-Shares (`sbi-tokens`), on member posts that already received the SBI Hive
vote. The call is its own Hive transaction. The contract never checks for a
matching Hive vote, and a bundle would let a rejected custom_json also reject
the SBI Hive vote.

Facts this module relies on, read from the contract source
(github.com/LasseEhlers/LasseCash-Migration-to-MAGI, contract/state/pob.go,
engine/pob.go) and checked against live state on 2026-10-23:

- Payload `<author>|<permlink>|<weightPct>`, weight 1..100. The author is used
  verbatim as a state key, so it must be the qualified "hive:<account>".
- The first vote on an unknown permlink registers it as a viral post when the
  author holds `post.threshold_viral` L-Shares. The contract never reads Hive:
  it checks no tag, no post existence and no age. A dry run on a permlink that
  does not exist registered it. The `lassecash` tag check here is therefore the
  only thing that keeps SBI from paying non-LasseCash posts out of the pool.
- A 100% vote costs 10% of a meter. The viral meter refills in 7 days, the deep
  meter in 30. Reward weight is L-Shares x weightPct / 1000, independent of the
  meter level.
- Every refusal aborts the call on Magi. Hive still includes the custom_json,
  so only Magi state shows whether the vote exists.
"""

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from hivesbi.magi import DEFAULT_MAGI_NODES, MagiClient, qualify

LASSECASH_TAG = "lassecash"
DEFAULT_CONTRACT_ID = "vsc1Be4TTjUiHgzhHAfqFn6s3PDAExH2X59fXV"
DEFAULT_VOTER = "sbi-tokens"
NET_ID = "vsc-mainnet"
CUSTOM_JSON_ID = "vsc.call"
VOTE_ACTION = "vote"

# Post windows, as the contract encodes them in field 0 of a post_ record.
VIRAL = 0
DEEP = 1
REGEN_DAYS = {VIRAL: 7, DEEP: 30}

# A full meter holds 1,000 weight-percent: a 100% vote costs 10% of it.
FULL_METER_WEIGHT_PCT = 1_000
LOOKBACK_DAYS = 30

# 1,000 L-Shares in the contract's 1e-8 base unit. Governance can move it
# (bounds 1 to 10,000 L-Shares); a dry run refusal "author needs N L-Shares"
# reports the live value. Override with config lassecash.threshold_viral.
DEFAULT_THRESHOLD_VIRAL = 1_000 * 10**8

# A qualified post stays a candidate this long after its Hive creation time.
# The SBI Hive vote lands within 24 hours, so the extra days only absorb
# deferrals for vote power or RC.
CANDIDATE_DAYS = 3

# Magi applies a Hive-included call within a few blocks. A row still without a
# verdict after this long is settled without one.
CONFIRM_TIMEOUT = timedelta(minutes=30)

# One vote per run. A dry run reads committed chain state, so a second vote in
# the same run would be checked against a meter that does not yet show the
# first one. The next run sees it.
MAX_VOTES_PER_RUN = 1

# rc_limit sizing, copied from the LasseCash signer (api/src/aioha-signer.ts
# sizeRc). Magi reserves the WHOLE rc_limit for five days whether the call
# succeeds or aborts, so the limit is sized from a dry run, not set generously.
# The simulator does not weigh state writes the way settlement does, hence the
# 3x headroom over the simulated gas.
GAS_PER_RC = 100_000
RC_HEADROOM = 3
RC_FLOOR = 1_500
RC_CEILING = 30_000

# Dry-run refusals that no later run can cure for the same post.
PERMANENT_REFUSALS = ("author needs", "voting has closed")

QUALIFIED = "QUALIFIED"
IGNORED = "IGNORED"
PENDING = "PENDING"
BROADCAST = "BROADCAST"
SUCCESS = "SUCCESS"
FAILED = "FAILED"
SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class LassecashSettings:
    enabled: bool = False
    broadcast: bool = False
    voter: str = DEFAULT_VOTER
    contract_id: str = DEFAULT_CONTRACT_ID
    magi_nodes: tuple = DEFAULT_MAGI_NODES
    threshold_viral: int = DEFAULT_THRESHOLD_VIRAL

    @classmethod
    def from_config(cls, cfg):
        """Read the optional `lassecash` block of config.json.

        A missing block means disabled. `enabled` runs the staker refresh and
        the vote job; `broadcast` is the separate switch that lets the job sign.
        """
        block = cfg.get("lassecash") or {}
        return cls(
            enabled=bool(block.get("enabled", False)),
            broadcast=bool(block.get("broadcast", False)),
            voter=block.get("voter", DEFAULT_VOTER),
            contract_id=block.get("contract_id", DEFAULT_CONTRACT_ID),
            magi_nodes=tuple(block.get("magi_nodes") or DEFAULT_MAGI_NODES),
            threshold_viral=int(block.get("threshold_viral", DEFAULT_THRESHOLD_VIRAL)),
        )


def utcnow():
    """Naive UTC at whole seconds, matching the DATETIME columns.

    The columns drop fractional seconds. A value with microseconds therefore
    reads back smaller than itself, and replace_stakers' `checked_at < now`
    cleanup would delete the snapshot it had just written.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


# --- contract keys and payloads ----------------------------------------------
def shares_key(account):
    return "shr_" + qualify(account)


def post_key(author, permlink):
    return f"post_{qualify(author)}_{permlink}"


def vote_key(author, permlink, voter):
    return f"pv_{qualify(author)}_{permlink}_{qualify(voter)}"


def build_vote_payload(author, permlink, weight):
    return f"{qualify(author)}|{permlink}|{int(weight)}"


def build_vote_json(contract_id, author, permlink, weight, rc_limit):
    """The custom_json body, field for field as lassecash.com sends it."""
    return {
        "net_id": NET_ID,
        "contract_id": contract_id,
        "action": VOTE_ACTION,
        "payload": build_vote_payload(author, permlink, weight),
        "rc_limit": int(rc_limit),
        "intents": [],
    }


def post_window(post_value):
    """Window of a post_ record; an unregistered post registers as viral."""
    if not post_value:
        return VIRAL
    try:
        return DEEP if int(post_value.split("|", 1)[0]) == DEEP else VIRAL
    except ValueError:
        return VIRAL


# --- vote weight and RC -------------------------------------------------------
def weight_budget(window):
    """Weight-percent one meter regenerates in LOOKBACK_DAYS: 4,285 viral, 1,000 deep."""
    return FULL_METER_WEIGHT_PCT * LOOKBACK_DAYS // REGEN_DAYS[window]


def compute_weight(window, qualified_posts):
    """Spread one meter's 30-day regeneration over the posts of the last 30 days.

    qualified_posts includes the post being voted. The contract accepts 1..100.
    """
    if qualified_posts < 1:
        qualified_posts = 1
    return max(1, min(100, weight_budget(window) // qualified_posts))


def size_rc_limit(gas_used):
    need = math.ceil(gas_used / GAS_PER_RC)
    return min(RC_CEILING, max(RC_FLOOR, need * RC_HEADROOM))


def is_permanent_refusal(err_msg):
    return any(marker in (err_msg or "") for marker in PERMANENT_REFUSALS)


# --- Hive post checks ---------------------------------------------------------
def has_lassecash_tag(comment):
    """True when the post carries the `lassecash` tag.

    Tags come from json_metadata. A root post's category is its first tag and
    is checked as well, because a community post keeps the community there.
    """
    tags = []
    try:
        tags.extend(comment["tags"] or [])
    except (KeyError, TypeError):
        pass
    metadata = getattr(comment, "json_metadata", None)
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except ValueError:
            metadata = None
    if isinstance(metadata, dict) and isinstance(metadata.get("tags"), list):
        tags.extend(metadata["tags"])
    try:
        tags.append(comment["category"])
    except (KeyError, TypeError):
        pass
    return any(
        isinstance(tag, str) and tag.strip().lower() == LASSECASH_TAG for tag in tags
    )


# --- lassecash_stakers ----------------------------------------------------------
def last_staker_refresh(conn):
    return conn.exec_driver_sql(
        "SELECT MAX(checked_at) FROM lassecash_stakers"
    ).scalar()


def replace_stakers(conn, shares_by_account, threshold, checked_at):
    """Write one complete snapshot: every member, including those with 0 shares.

    Keeping the zero rows makes MAX(checked_at) a reliable refresh time even
    when no member stakes. Rows of accounts that left SBI are deleted.
    """
    rows = [
        (account, shares, 1 if shares >= threshold else 0, checked_at)
        for account, shares in shares_by_account.items()
    ]
    if rows:
        conn.exec_driver_sql(
            """
            INSERT INTO lassecash_stakers (account, l_shares, eligible, checked_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                l_shares = VALUES(l_shares),
                eligible = VALUES(eligible),
                checked_at = VALUES(checked_at)
            """,
            rows,
        )
    conn.exec_driver_sql(
        "DELETE FROM lassecash_stakers WHERE checked_at < %s", (checked_at,)
    )


def eligible_stakers(conn):
    rows = conn.exec_driver_sql(
        "SELECT account FROM lassecash_stakers WHERE eligible = 1"
    ).fetchall()
    return [row[0] for row in rows]


def sync_lassecash_stakers(db2, magi, settings, members, refresh_min, now=None):
    """Refresh lassecash_stakers from Magi, at most once per `refresh_min`.

    hsbi_token_snapshot calls this inside its cycle gate. In the high-VP regime
    that gate opens about every 15 minutes, so the guard here keeps the full
    member scan to one per share_cycle_min (144 minutes).

    Returns the number of eligible members, or None when the guard skipped.
    """
    now = now or utcnow()
    with db2.engine.begin() as conn:
        last = last_staker_refresh(conn)
    if last is not None and now - last < timedelta(minutes=refresh_min):
        return None

    members = list(dict.fromkeys(m for m in members if m))
    state = magi.get_state(settings.contract_id, [shares_key(m) for m in members])
    shares = {m: int(state.get(shares_key(m), 0)) for m in members}

    with db2.engine.begin() as conn:
        replace_stakers(conn, shares, settings.threshold_viral, now)
    return sum(1 for value in shares.values() if value >= settings.threshold_viral)


def refresh_lassecash_stakers(rt, refresh_min, magi=None):
    """Entry point for hsbi_token_snapshot. Never raises.

    The refresh runs after issuance, and a Magi outage must not turn into a
    failed token job, so every error is printed and swallowed here.
    """
    try:
        settings = LassecashSettings.from_config(rt["cfg"])
        if not settings.enabled:
            return None
        members = rt["storages"]["member"].get_all_accounts()
        eligible = sync_lassecash_stakers(
            rt["db2"],
            magi or MagiClient(settings.magi_nodes),
            settings,
            members,
            refresh_min,
        )
        if eligible is not None:
            print(
                f"lassecash: staker list refreshed, {eligible} of {len(members)} "
                "members meet the viral threshold"
            )
        return eligible
    except Exception as exc:  # noqa: BLE001 - must never break issuance
        print(f"lassecash: staker refresh failed: {exc}")
        return None


# --- lassecash_votes ------------------------------------------------------------
_VOTE_COLUMNS = (
    "id, authorperm, author, permlink, post_created, voter, vote_window, "
    "weight, rc_limit, trx_id, status, error_message, created_at, updated_at"
)


def _vote_rows(result):
    names = [c.strip() for c in _VOTE_COLUMNS.split(",")]
    return [dict(zip(names, row)) for row in result.fetchall()]


def known_authorperms(conn, authorperms):
    if not authorperms:
        return set()
    placeholders = ", ".join(["%s"] * len(authorperms))
    rows = conn.exec_driver_sql(
        f"SELECT authorperm FROM lassecash_votes WHERE authorperm IN ({placeholders})",
        tuple(authorperms),
    ).fetchall()
    return {row[0] for row in rows}


def insert_vote_row(conn, authorperm, author, permlink, post_created, voter, status, now):
    conn.exec_driver_sql(
        """
        INSERT INTO lassecash_votes
            (authorperm, author, permlink, post_created, voter, vote_window,
             status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (authorperm, author, permlink, post_created, voter, VIRAL, status, now, now),
    )


def vote_rows_with_status(conn, statuses):
    placeholders = ", ".join(["%s"] * len(statuses))
    return _vote_rows(
        conn.exec_driver_sql(
            f"SELECT {_VOTE_COLUMNS} FROM lassecash_votes "
            f"WHERE status IN ({placeholders}) ORDER BY post_created, id",
            tuple(statuses),
        )
    )


def update_vote_row(conn, row_id, now, **fields):
    """Set `fields` and updated_at on one row. updated_at times the settle timeout."""
    assignments = ", ".join(f"{name} = %s" for name in fields)
    conn.exec_driver_sql(
        f"UPDATE lassecash_votes SET {assignments}, updated_at = %s WHERE id = %s",
        (*fields.values(), now, row_id),
    )


def qualified_count(conn, window, since):
    """Qualifying posts of one window in the lookback, whatever their vote outcome."""
    return conn.exec_driver_sql(
        """
        SELECT COUNT(*) FROM lassecash_votes
        WHERE vote_window = %s AND created_at >= %s AND status <> %s
        """,
        (window, since, IGNORED),
    ).scalar()


def expire_candidates(conn, cutoff, now):
    """Skip QUALIFIED rows whose post left the candidate window. Keeps the last error."""
    return conn.exec_driver_sql(
        """
        UPDATE lassecash_votes
        SET status = %s,
            error_message = CONCAT('candidate window ended: ', COALESCE(error_message, 'no vote cast')),
            updated_at = %s
        WHERE status = %s AND post_created < %s
        """,
        (SKIPPED, now, QUALIFIED, cutoff),
    ).rowcount


def recent_voted_posts(conn, authors, since):
    """Root posts in posts_comments (sbi_steem_ops) that SBI voted, by `authors`."""
    if not authors:
        return []
    placeholders = ", ".join(["%s"] * len(authors))
    rows = conn.exec_driver_sql(
        f"""
        SELECT authorperm, author, created FROM posts_comments
        WHERE voted = 1 AND main_post = 1 AND created >= %s
          AND author IN ({placeholders})
        ORDER BY created
        """,
        (since, *authors),
    ).fetchall()
    return [{"authorperm": r[0], "author": r[1], "created": r[2]} for r in rows]
