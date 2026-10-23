"""Cast the LasseCash (L-cash) companion vote on SBI members' `lassecash` posts.

Runs after hsbi_upvote_post_comment in sbirunner.sh. Each run:

1. settle_votes   - resolve earlier PENDING/BROADCAST rows from Magi state.
2. qualify_posts  - record each new SBI-voted root post by a member on the
                    staker list (refreshed by hsbi_token_snapshot), as
                    QUALIFIED when it carries the `lassecash` tag, else IGNORED.
3. cast_votes     - dry-run the oldest QUALIFIED post on Magi and, when the
                    contract accepts it, broadcast one vsc.call vote.

Config (config.json, block `lassecash`): `enabled` turns the job on;
`broadcast` lets it sign. With broadcast off the job only reports the votes it
would cast (shadow run). The job never reads L-Shares itself: the staker list
is refreshed on the token cycle, so this job adds no member scan to the vote
loop.

LIVE: with broadcast on, this job signs custom_json with the voter's posting key.
"""

import time
from datetime import timedelta

from nectar.comment import Comment
from nectar.utils import resolve_authorperm

from hivesbi.issuance_log import issue_trx_id, never_reached_chain
from hivesbi.lassecash import (
    BROADCAST,
    CANDIDATE_DAYS,
    CONFIRM_TIMEOUT,
    CUSTOM_JSON_ID,
    FAILED,
    IGNORED,
    LOOKBACK_DAYS,
    MAX_VOTES_PER_RUN,
    PENDING,
    QUALIFIED,
    SKIPPED,
    SUCCESS,
    VOTE_ACTION,
    LassecashSettings,
    build_vote_json,
    build_vote_payload,
    compute_weight,
    eligible_stakers,
    expire_candidates,
    has_lassecash_tag,
    insert_vote_row,
    is_permanent_refusal,
    known_authorperms,
    post_key,
    post_window,
    qualified_count,
    recent_voted_posts,
    size_rc_limit,
    update_vote_row,
    utcnow,
    vote_key,
    vote_rows_with_status,
)
from hivesbi.magi import MagiClient
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import KeysDB

LOG = "hsbi_lassecash_vote"


def load_comment(authorperm, hv):
    """Fetch a post, rotating nodes on failure. None means: try again next run."""
    for _ in range(5):
        try:
            return Comment(authorperm, blockchain_instance=hv)
        except Exception:
            hv.rpc.next()
    return None


def settle_votes(db2, magi, settings, now=None):
    """Resolve PENDING and BROADCAST rows.

    The vote record (pv_ key) is the proof a vote exists, whatever the row
    says. Without it, a BROADCAST row with a trx id asks Magi for the call's
    verdict. A row that has neither after CONFIRM_TIMEOUT is settled without
    one: a PENDING row never got a trx id, so it returns to QUALIFIED and the
    next dry run decides again; a BROADCAST row is FAILED.
    """
    now = now or utcnow()
    with db2.engine.begin() as conn:
        rows = vote_rows_with_status(conn, (PENDING, BROADCAST))
    if not rows:
        return
    state = magi.get_state(
        settings.contract_id,
        [vote_key(r["author"], r["permlink"], r["voter"]) for r in rows],
    )
    for row in rows:
        update = None
        if vote_key(row["author"], row["permlink"], row["voter"]) in state:
            update = dict(status=SUCCESS, error_message=None)
        elif row["status"] == BROADCAST and row["trx_id"]:
            verdict, error = magi.transaction_verdict(row["trx_id"])
            if verdict == "CONFIRMED":
                update = dict(status=SUCCESS, error_message=None)
            elif verdict == "FAILED":
                update = dict(status=FAILED, error_message=error or "Magi verdict FAILED")
        if update is None and now - row["updated_at"] >= CONFIRM_TIMEOUT:
            if row["status"] == PENDING:
                update = dict(
                    status=QUALIFIED,
                    error_message="broadcast unconfirmed after timeout; retrying",
                )
            else:
                update = dict(status=FAILED, error_message="no Magi verdict after timeout")
        if update is None:
            continue
        with db2.engine.begin() as conn:
            update_vote_row(conn, row["id"], now, **update)
        print(f"{LOG}: {row['authorperm']} -> {update['status']} {update['error_message'] or ''}")


def qualify_posts(db, db2, settings, load, now=None):
    """Record each new SBI-voted root post of an eligible staker. Returns rows added.

    A post without the `lassecash` tag is stored as IGNORED, so the next run
    does not fetch it again. A post that cannot be fetched is left for the
    next run.
    """
    now = now or utcnow()
    with db2.engine.begin() as conn:
        authors = eligible_stakers(conn)
    if not authors:
        return 0
    with db.engine.begin() as conn:
        posts = recent_voted_posts(conn, authors, now - timedelta(days=CANDIDATE_DAYS))
    with db2.engine.begin() as conn:
        known = known_authorperms(conn, [p["authorperm"] for p in posts])

    added = 0
    for post in posts:
        if post["authorperm"] in known:
            continue
        comment = load(post["authorperm"])
        if comment is None:
            continue
        tagged = comment.is_main_post() and has_lassecash_tag(comment)
        _, permlink = resolve_authorperm(post["authorperm"])
        with db2.engine.begin() as conn:
            insert_vote_row(
                conn,
                post["authorperm"],
                post["author"],
                permlink,
                post["created"],
                settings.voter,
                QUALIFIED if tagged else IGNORED,
                now,
            )
        added += 1
        if tagged:
            print(f"{LOG}: qualified {post['authorperm']}")
    return added


def cast_votes(db2, magi, settings, broadcast_vote, now=None):
    """Vote on QUALIFIED posts, oldest first. Returns the number of broadcasts.

    broadcast_vote(json_body) signs and broadcasts the custom_json and returns
    nectar's transaction dict. It is only called with settings.broadcast on.
    """
    now = now or utcnow()
    with db2.engine.begin() as conn:
        expired = expire_candidates(conn, now - timedelta(days=CANDIDATE_DAYS), now)
        rows = vote_rows_with_status(conn, (QUALIFIED,))
    if expired:
        print(f"{LOG}: {expired} candidate(s) expired without a vote")
    if not rows:
        return 0

    keys = []
    for r in rows:
        keys.append(post_key(r["author"], r["permlink"]))
        keys.append(vote_key(r["author"], r["permlink"], settings.voter))
    state = magi.get_state(settings.contract_id, keys)
    available_rc = None
    cast = 0

    for row in rows:
        if cast >= MAX_VOTES_PER_RUN:
            break
        author, permlink = row["author"], row["permlink"]
        # The window is recorded first, so every row, voted or skipped, counts
        # against the meter of its own window in later weight calculations.
        window = post_window(state.get(post_key(author, permlink)))

        if vote_key(author, permlink, settings.voter) in state:
            # A vote already exists (for example one cast on lassecash.com). A
            # second call would replace its weight and spend vote power again.
            with db2.engine.begin() as conn:
                update_vote_row(conn, row["id"], now, status=SKIPPED, vote_window=window,
                                error_message="voter already holds a vote on this post")
            continue

        with db2.engine.begin() as conn:
            update_vote_row(conn, row["id"], now, vote_window=window)
            posts_in_lookback = qualified_count(
                conn, window, now - timedelta(days=LOOKBACK_DAYS)
            )
        weight = compute_weight(window, posts_in_lookback)

        dry_run = magi.simulate_call(
            settings.voter,
            settings.contract_id,
            VOTE_ACTION,
            build_vote_payload(author, permlink, weight),
        )
        if not dry_run["success"]:
            refusal = dry_run["err_msg"].split("\n", 1)[0] or "dry run refused"
            status = SKIPPED if is_permanent_refusal(refusal) else QUALIFIED
            with db2.engine.begin() as conn:
                update_vote_row(conn, row["id"], now, status=status, weight=weight,
                                error_message=refusal)
            print(f"{LOG}: {row['authorperm']} dry run refused ({status}): {refusal}")
            continue

        rc_limit = size_rc_limit(dry_run["gas_used"])
        if available_rc is None:
            available_rc = magi.available_rc(settings.voter)
        if available_rc is not None and available_rc < rc_limit:
            # Magi refuses a call whose rc_limit exceeds available RC. Every
            # later row needs RC too, so stop until the reservation thaws.
            with db2.engine.begin() as conn:
                update_vote_row(conn, row["id"], now, weight=weight, rc_limit=rc_limit,
                                error_message=f"RC short: available {available_rc}, need {rc_limit}")
            print(f"{LOG}: RC short ({available_rc} < {rc_limit}); deferring")
            break

        body = build_vote_json(settings.contract_id, author, permlink, weight, rc_limit)
        if not settings.broadcast:
            with db2.engine.begin() as conn:
                update_vote_row(conn, row["id"], now, weight=weight, rc_limit=rc_limit,
                                error_message="shadow run: broadcast disabled")
            print(f"{LOG}: shadow run, would vote {row['authorperm']} at {weight}% "
                  f"(rc_limit {rc_limit}, window {window}, {posts_in_lookback} posts in lookback)")
            continue

        # Write-ahead: the row is PENDING before the one step that cannot be
        # rolled back. settle_votes resolves a PENDING row that never advanced.
        with db2.engine.begin() as conn:
            update_vote_row(conn, row["id"], now, status=PENDING, weight=weight,
                            rc_limit=rc_limit, error_message=None)
        cast += 1
        try:
            tx = broadcast_vote(body)
        except Exception as exc:
            error = str(exc)
            # A provable rejection returns the post to the queue at once. Any
            # other error may hide a landed broadcast, so the row stays
            # PENDING until settle_votes finds the vote or times out.
            status = QUALIFIED if never_reached_chain(error) else PENDING
            with db2.engine.begin() as conn:
                update_vote_row(conn, row["id"], now, status=status, error_message=error)
            print(f"{LOG}: broadcast failed for {row['authorperm']}: {error}")
            continue
        trx_id = issue_trx_id(tx or {})
        with db2.engine.begin() as conn:
            update_vote_row(conn, row["id"], now, status=BROADCAST,
                            trx_id=None if trx_id == "N/A" else trx_id)
        print(f"{LOG}: voted {row['authorperm']} at {weight}% (rc_limit {rc_limit}, trx {trx_id})")
    return cast


def run():
    start = time.time()
    rt = get_runtime()
    cfg = rt["cfg"]
    settings = LassecashSettings.from_config(cfg)
    if not settings.enabled:
        print(f"{LOG}: disabled (config.json lassecash.enabled)")
        return

    keys = []
    if settings.broadcast:
        key_row = KeysDB(rt["db2"]).get(settings.voter, "posting")
        if key_row is None:
            print(f"{LOG}: no posting key stored for {settings.voter}; not voting")
            return
        keys.append(key_row["wif"].replace("\n", "").replace("\r", ""))
    hv = make_hive(cfg, keys=keys, num_retries=5, call_num_retries=3, timeout=15,
                   nobroadcast=not settings.broadcast)
    magi = MagiClient(settings.magi_nodes)

    def broadcast_vote(body):
        return hv.custom_json(CUSTOM_JSON_ID, body, required_posting_auths=[settings.voter])

    settle_votes(rt["db2"], magi, settings)
    qualify_posts(rt["db"], rt["db2"], settings, lambda ap: load_comment(ap, hv))
    cast_votes(rt["db2"], magi, settings, broadcast_vote)
    print(f"{LOG}: run took {time.time() - start:.2f} s")


if __name__ == "__main__":
    run()
