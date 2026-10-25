"""LasseCash companion votes on SBI members' `lassecash` posts.

LasseCash rewards posts from a contract on Magi (formerly VSC). A Hive vote
never reaches that contract: only a `vsc.call` custom_json with action `vote`
counts. hsbi_lassecash_vote casts that call from one account that holds
L-Shares (`sbi-tokens`) on the `lassecash` root posts of members who meet the
viral threshold:

- A post that received the SBI Hive vote gets the L-cash vote in the same
  pipeline run, whatever the author's last L-cash vote.
- A post without the SBI Hive vote becomes a candidate when the upvote job's
  24 hour window closes. It gets the L-cash vote only when the author's last
  L-cash vote attempt is at least 24 hours old (LCASH_CAP).

The call is its own Hive transaction. The contract never checks for a matching
Hive vote, and a bundle would let a rejected custom_json also reject the SBI
Hive vote.

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

# Hive produces one block every BLOCK_INTERVAL_S seconds. The backfill turns
# LOOKBACK_DAYS into a block range for Magi's contract call history.
BLOCK_INTERVAL_S = 3
BLOCKS_PER_DAY = 86_400 // BLOCK_INTERVAL_S

# 1,000 L-Shares in the contract's 1e-8 base unit. Governance can move it
# (bounds 1 to 10,000 L-Shares); a dry run refusal "author needs N L-Shares"
# reports the live value. Override with config lassecash.threshold_viral.
DEFAULT_THRESHOLD_VIRAL = 1_000 * 10**8

# hsbi_upvote_post_comment votes a post only in its first 24 hours. A post
# without the SBI Hive vote by then never gets one, and becomes an L-cash
# candidate only then.
HIVE_VOTE_WINDOW = timedelta(hours=24)

# A post without the SBI Hive vote gets the L-cash vote only when the author's
# last L-cash vote attempt is at least this old. Hive-voted posts bypass the
# cap. A broadcast that provably never reached the chain is not an attempt.
LCASH_CAP = timedelta(hours=24)

# A post stays a candidate this long after its Hive creation time. A Hive-voted
# post qualifies within minutes and a post without the Hive vote at 24 hours,
# so the remaining days absorb deferrals for the cap, vote power or RC.
CANDIDATE_DAYS = 3

# Transaction expiration in seconds. hsbi_lassecash_vote passes it to
# make_hive. A transaction that no block includes before it expires can never
# be included.
TX_EXPIRATION_S = 300

# Magi applies a Hive-included call within a few blocks. A BROADCAST row still
# without a verdict after this long is settled without one. A PENDING row is
# checked against account history only after this long, so the timeout must
# exceed TX_EXPIRATION_S.
CONFIRM_TIMEOUT = timedelta(minutes=30)

# The account-history search for a PENDING row's broadcast starts this long
# before broadcast_at, and needs irreversible blocks this long past the
# transaction's expiration, to absorb clock skew between this host and Hive.
BROADCAST_SEARCH_MARGIN = timedelta(minutes=10)

# One vote at a time. A dry run reads committed Magi state, so the meter that a
# dry run checks lacks every vote Magi has not applied yet. cast_votes casts at
# most one vote per run, and none while an earlier vote is PENDING or BROADCAST.
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
BASELINE = "BASELINE"
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


def is_vote_op(op, contract_id, voter, author, permlink):
    """True when an account-history op is the voter's vote call on this post.

    op is a nectar history dict of a custom_json. The weight is not compared:
    any vote call on the post proves that a broadcast reached the chain.
    """
    auths = list(op.get("required_posting_auths") or []) + list(op.get("required_auths") or [])
    if op.get("id") != CUSTOM_JSON_ID or voter not in auths:
        return False
    body = op.get("json")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError:
            return False
    if not isinstance(body, dict):
        return False
    prefix = f"{qualify(author)}|{permlink}|"
    return (
        body.get("contract_id") == contract_id
        and body.get("action") == VOTE_ACTION
        and str(body.get("payload", "")).startswith(prefix)
    )


# --- vote weight, cap and RC --------------------------------------------------
def weight_budget(window):
    """Weight-percent one meter regenerates in LOOKBACK_DAYS: 4,285 viral, 1,000 deep."""
    return FULL_METER_WEIGHT_PCT * LOOKBACK_DAYS // REGEN_DAYS[window]


def compute_weight(window, votes):
    """Spread one meter's 30-day regeneration over the votes of the last 30 days.

    votes includes the vote being cast. The contract accepts 1..100.
    """
    if votes < 1:
        votes = 1
    return max(1, min(100, weight_budget(window) // votes))


def size_rc_limit(gas_used):
    need = math.ceil(gas_used / GAS_PER_RC)
    return min(RC_CEILING, max(RC_FLOOR, need * RC_HEADROOM))


def is_permanent_refusal(err_msg):
    return any(marker in (err_msg or "") for marker in PERMANENT_REFUSALS)


def baseline_vote_time(post):
    """When the live job votes a post: with the Hive vote, else when the window closes."""
    if post["hive_voted"]:
        return post["created"]
    return post["created"] + HIVE_VOTE_WINDOW


def apply_cap(posts):
    """Split posts into (counted, capped) by the cap, at each post's own vote time.

    Each post carries "author", "created" and "hive_voted". A Hive-voted post
    always counts. Another post counts only when the author's previous counted
    vote is at least LCASH_CAP older than its own vote time. The live job
    defers a capped post instead and votes it when the cap ends inside its
    candidate window, so this split counts fewer votes than the live job casts.
    """
    counted, capped = [], []
    last_vote = {}
    for post in sorted(posts, key=baseline_vote_time):
        at = baseline_vote_time(post)
        previous = last_vote.get(post["author"])
        if post["hive_voted"] or previous is None or at - previous >= LCASH_CAP:
            counted.append(post)
            last_vote[post["author"]] = at
        else:
            capped.append(post)
    return counted, capped


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


def sbi_voted(comment, sbi_voters):
    """True when one of SBI's voting accounts voted the post on Hive.

    posts_comments.voted stays 0 when a pooled SBI vote lands after the upvote
    job's last pass over the post, so the post's own votes decide.
    """
    voters = set(sbi_voters)
    return any(vote["voter"] in voters for vote in comment.get_votes())


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


def blacklisted_members(conn, accounts):
    """Members that hsbi_upvote_post_comment never votes, for policy reasons.

    The upvote job skips a member whose `blacklisted` flag is set, or whose flag
    is NULL while `hivewatchers` or `buildawhale` is set. A post without the
    SBI Hive vote by such a member gets no L-cash vote either.
    """
    if not accounts:
        return set()
    placeholders = ", ".join(["%s"] * len(accounts))
    rows = conn.exec_driver_sql(
        f"""
        SELECT account FROM member
        WHERE account IN ({placeholders})
          AND (COALESCE(blacklisted, 0) <> 0
               OR (blacklisted IS NULL
                   AND (COALESCE(hivewatchers, 0) <> 0 OR COALESCE(buildawhale, 0) <> 0)))
        """,
        tuple(accounts),
    ).fetchall()
    return {row[0] for row in rows}


# --- lassecash_votes ------------------------------------------------------------
_VOTE_COLUMNS = (
    "id, authorperm, author, permlink, post_created, voter, hive_voted, vote_window, "
    "weight, rc_limit, trx_id, broadcast_at, status, error_message, created_at, updated_at"
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


def insert_vote_row(conn, authorperm, author, permlink, post_created, voter, status, now,
                    hive_voted=False, vote_window=VIRAL, created_at=None, error_message=None):
    """Insert one row. created_at defaults to now; a BASELINE row passes its vote time."""
    conn.exec_driver_sql(
        """
        INSERT INTO lassecash_votes
            (authorperm, author, permlink, post_created, voter, hive_voted, vote_window,
             status, error_message, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (authorperm, author, permlink, post_created, voter, 1 if hive_voted else 0,
         vote_window, status, error_message, created_at or now, now),
    )


def vote_rows_with_status(conn, statuses):
    """Rows with one of `statuses`: Hive-voted rows first, then oldest post first.

    cast_votes votes the first row that passes every check, so a post with the
    SBI Hive vote goes ahead of every post without it.
    """
    placeholders = ", ".join(["%s"] * len(statuses))
    return _vote_rows(
        conn.exec_driver_sql(
            f"SELECT {_VOTE_COLUMNS} FROM lassecash_votes "
            f"WHERE status IN ({placeholders}) ORDER BY hive_voted DESC, post_created, id",
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


def in_flight_count(conn):
    """Votes broadcast or about to be, that Magi has not settled yet."""
    return conn.exec_driver_sql(
        "SELECT COUNT(*) FROM lassecash_votes WHERE status IN (%s, %s)",
        (PENDING, BROADCAST),
    ).scalar()


def last_attempt(conn, author):
    """Time of the author's last L-cash vote attempt, or None."""
    return conn.exec_driver_sql(
        "SELECT MAX(broadcast_at) FROM lassecash_votes WHERE author = %s",
        (author,),
    ).scalar()


def meter_votes(conn, window, since):
    """Votes charged to one window's meter in the lookback.

    A live row counts from its broadcast attempt, unless Magi failed it. A
    BASELINE row from the backfill counts from its created_at, which holds
    the time the live job would have voted the post.
    """
    return conn.exec_driver_sql(
        """
        SELECT COUNT(*) FROM lassecash_votes
        WHERE vote_window = %s
          AND ((status = %s AND created_at >= %s)
               OR (broadcast_at >= %s AND status <> %s))
        """,
        (window, BASELINE, since, since, FAILED),
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


def member_root_posts(conn, authors, since, until=None):
    """Root posts in posts_comments (sbi_steem_ops) by `authors`, oldest first.

    voted is the upvote job's flag for the SBI Hive vote. skip marks a post
    that the stream's tag, app or body blacklist rejected.
    """
    if not authors:
        return []
    placeholders = ", ".join(["%s"] * len(authors))
    until_clause = "AND created < %s" if until is not None else ""
    params = (since, *(() if until is None else (until,)), *authors)
    rows = conn.exec_driver_sql(
        f"""
        SELECT authorperm, author, created, voted, skip FROM posts_comments
        WHERE main_post = 1 AND created >= %s {until_clause}
          AND author IN ({placeholders})
        ORDER BY created
        """,
        params,
    ).fetchall()
    return [
        {"authorperm": r[0], "author": r[1], "created": r[2],
         "voted": bool(r[3]), "skip": bool(r[4])}
        for r in rows
    ]
