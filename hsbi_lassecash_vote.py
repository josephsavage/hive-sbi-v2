"""Cast the LasseCash (L-cash) companion vote on SBI members' `lassecash` posts.

Runs after hsbi_upvote_post_comment in sbirunner.sh. Each run:

1. settle_votes   - resolve earlier PENDING/BROADCAST rows from Magi state and,
                    for a PENDING row, from the voter's Hive account history.
2. qualify_posts  - record each new root post of a member on the staker list
                    (refreshed by hsbi_token_snapshot): a post with the SBI Hive
                    vote at once, a post without it once the upvote job's 24 hour
                    window has closed. QUALIFIED when it carries the `lassecash`
                    tag, else IGNORED.
3. cast_votes     - dry-run the QUALIFIED posts on Magi, Hive-voted posts first
                    and then the oldest first, and broadcast one vsc.call vote
                    for the first post the contract accepts. A post without the
                    SBI Hive vote waits for the author's 24 hour cap. No vote
                    goes out while an earlier vote is unsettled.

A failed phase prints its error and the next phase still runs.

Config (config.json, block `lassecash`): `enabled` turns the job on;
`broadcast` lets it sign. With broadcast off the job only reports the votes it
would cast (shadow run). The job never reads L-Shares itself: the staker list
is refreshed on the token cycle, so this job adds no member scan to the vote
loop.

`--backfill` seeds the weight lookback once, at deploy, from the last 30 days
of posts, and prints how the tagged posts compare with the posts the contract
registered. It refreshes the staker list first and never signs.

LIVE: with broadcast on, this job signs custom_json with the voter's posting key.
"""

import argparse
import time
from datetime import timedelta, timezone

from nectar.account import Account
from nectar.comment import Comment
from nectar.utils import parse_time, resolve_authorperm

from hivesbi.issuance_log import issue_trx_id, never_reached_chain
from hivesbi.lassecash import (
    BASELINE,
    BLOCK_INTERVAL_S,
    BLOCKS_PER_DAY,
    BROADCAST,
    BROADCAST_SEARCH_MARGIN,
    CANDIDATE_DAYS,
    CONFIRM_TIMEOUT,
    CUSTOM_JSON_ID,
    FAILED,
    HIVE_VOTE_WINDOW,
    IGNORED,
    LCASH_CAP,
    LOOKBACK_DAYS,
    MAX_VOTES_PER_RUN,
    PENDING,
    QUALIFIED,
    SKIPPED,
    SUCCESS,
    TX_EXPIRATION_S,
    VOTE_ACTION,
    LassecashSettings,
    apply_cap,
    baseline_vote_time,
    blacklisted_members,
    build_vote_json,
    build_vote_payload,
    compute_weight,
    eligible_stakers,
    expire_candidates,
    has_lassecash_tag,
    in_flight_count,
    insert_vote_row,
    is_permanent_refusal,
    is_vote_op,
    known_authorperms,
    last_attempt,
    member_root_posts,
    meter_votes,
    post_key,
    post_window,
    sbi_voted,
    size_rc_limit,
    sync_lassecash_stakers,
    update_vote_row,
    utcnow,
    vote_key,
    vote_rows_with_status,
)
from hivesbi.magi import MagiClient, MagiError, qualify
from hivesbi.settings import get_runtime, make_hive

LOG = "hsbi_lassecash_vote"


def load_comment(authorperm, hv):
    """Fetch a post, rotating nodes on failure. None means: try again next run."""
    for _ in range(5):
        try:
            return Comment(authorperm, blockchain_instance=hv)
        except Exception:
            hv.rpc.next()
    return None


class HistoryNotCovered(Exception):
    """The voter's account history cannot prove yet that a vote call is absent."""


def chain_time(value):
    """Naive UTC datetime of a Hive time string, as the DATETIME columns hold it."""
    if isinstance(value, str):
        value = parse_time(value)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def last_irreversible_time(hv):
    """Block time of the node's last irreversible block, as naive UTC.

    The dynamic global properties hold the head block time only, so the time
    is the head time less one block interval per block between the two.
    """
    props = hv.get_dynamic_global_properties()
    behind = props["head_block_number"] - props["last_irreversible_block_num"]
    return chain_time(props["time"]) - timedelta(seconds=BLOCK_INTERVAL_S * behind)


def find_broadcast(hv, settings, row):
    """Return the trx_id of the voter's vote call on the row's post, or None.

    Searches the voter's Hive account history back to the row's broadcast
    attempt. None proves that the broadcast never reached the chain, so the
    search returns None only from a history that covers the transaction's
    whole life:

    - the node's last irreversible block is past the transaction's expiration;
    - the walk reaches an operation older than the search start.

    Otherwise it raises HistoryNotCovered and the row stays PENDING. A node can
    answer a lagging history, or an empty one that nectar walks as an account
    without operations.
    """
    attempted = row["broadcast_at"] or row["updated_at"]
    since = attempted - BROADCAST_SEARCH_MARGIN
    expired = attempted + timedelta(seconds=TX_EXPIRATION_S) + BROADCAST_SEARCH_MARGIN
    irreversible = last_irreversible_time(hv)
    if irreversible <= expired:
        raise HistoryNotCovered(
            f"last irreversible block {irreversible:%Y-%m-%d %H:%M:%S} is not past "
            f"the expiration margin {expired:%Y-%m-%d %H:%M:%S}"
        )
    account = Account(settings.voter, blockchain_instance=hv)
    for op in account.history_reverse(only_ops=["custom_json"]):
        if chain_time(op["timestamp"]) < since:
            return None
        if is_vote_op(op, settings.contract_id, settings.voter, row["author"], row["permlink"]):
            return op["trx_id"]
    raise HistoryNotCovered(f"account history ends after {since:%Y-%m-%d %H:%M:%S}")


def _settle_row(row, state, magi, find, now):
    """Return the update that settles one PENDING or BROADCAST row, or None to wait."""
    if vote_key(row["author"], row["permlink"], row["voter"]) in state:
        return dict(status=SUCCESS, error_message=None)

    if row["status"] == BROADCAST:
        error = None
        if row["trx_id"]:
            try:
                verdict, error = magi.transaction_verdict(row["trx_id"])
            except MagiError as exc:
                verdict, error = None, f"verdict lookup failed: {exc}"
            if verdict == "CONFIRMED":
                return dict(status=SUCCESS, error_message=None)
            if verdict == "FAILED":
                return dict(status=FAILED, error_message=error or "Magi verdict FAILED")
        if now - row["updated_at"] >= CONFIRM_TIMEOUT:
            reason = "no Magi verdict after timeout"
            return dict(status=FAILED, error_message=f"{reason}; {error}" if error else reason)
        return None

    # PENDING: the broadcast raised an ambiguous error, or the job stopped
    # between the write-ahead and the broadcast. Only account history after
    # the transaction expired tells which.
    attempted = row["broadcast_at"] or row["updated_at"]
    if now - attempted < CONFIRM_TIMEOUT:
        return None
    trx_id = find(row)
    if trx_id:
        return dict(status=BROADCAST, trx_id=trx_id, error_message=None)
    return dict(status=QUALIFIED, broadcast_at=None,
                error_message="broadcast expired without reaching the chain; retrying")


def settle_votes(db2, magi, settings, find, now=None):
    """Resolve PENDING and BROADCAST rows.

    The vote record (pv_ key) is the proof a vote exists, whatever the row
    says. Without it, a BROADCAST row with a trx id asks Magi for the call's
    verdict, and is FAILED after CONFIRM_TIMEOUT without one. A PENDING row
    is searched for in the voter's account history (find): a found call
    becomes BROADCAST with its trx id, and a call that the history proves
    absent returns the post to QUALIFIED. A history that proves nothing
    (HistoryNotCovered) or a lookup error leaves the row PENDING for the next
    run, and an error on one row never stops the others.
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
        try:
            update = _settle_row(row, state, magi, find, now)
        except HistoryNotCovered as exc:
            print(f"{LOG}: {row['authorperm']} stays {row['status']}: {exc}")
            continue
        except Exception as exc:  # noqa: BLE001 - one row must not block the others
            print(f"{LOG}: settling {row['authorperm']} failed, row stays {row['status']}: {exc}")
            continue
        if update is None:
            continue
        with db2.engine.begin() as conn:
            update_vote_row(conn, row["id"], now, **update)
        print(f"{LOG}: {row['authorperm']} -> {update['status']} {update['error_message'] or ''}")


def _qualify_post(db2, settings, load, sbi_voters, post, blacklisted, now):
    """Record one post. Returns 1 when a row was added, 0 when the post waits."""
    _, permlink = resolve_authorperm(post["authorperm"])
    hive_voted = post["voted"]
    note = None
    if not hive_voted and post["author"] in blacklisted:
        status, note = IGNORED, "member is excluded from SBI votes"
    else:
        comment = load(post["authorperm"])
        if comment is None:
            return 0
        if not hive_voted:
            hive_voted = sbi_voted(comment, sbi_voters)
        tagged = comment.is_main_post() and has_lassecash_tag(comment)
        status = QUALIFIED if tagged else IGNORED
    with db2.engine.begin() as conn:
        insert_vote_row(conn, post["authorperm"], post["author"], permlink, post["created"],
                        settings.voter, status, now, hive_voted=hive_voted, error_message=note)
    if status == QUALIFIED:
        kind = "Hive-voted" if hive_voted else "no Hive vote, capped"
        print(f"{LOG}: qualified {post['authorperm']} ({kind})")
    return 1


def qualify_posts(db, db2, settings, load, sbi_voters, now=None):
    """Record each new root post of an eligible staker. Returns rows added.

    A post with the SBI Hive vote is recorded at once. A post without it is
    recorded once the upvote job's window (HIVE_VOTE_WINDOW) has closed,
    unless the stream's blacklist skipped it or the member is excluded from
    SBI votes. A post without the `lassecash` tag is stored as IGNORED, so
    the next run does not fetch it again. A post that cannot be fetched is
    left for the next run.
    """
    now = now or utcnow()
    with db2.engine.begin() as conn:
        authors = eligible_stakers(conn)
    if not authors:
        return 0
    with db.engine.begin() as conn:
        posts = member_root_posts(conn, authors, now - timedelta(days=CANDIDATE_DAYS))
    window_end = now - HIVE_VOTE_WINDOW
    posts = [p for p in posts if p["voted"] or (not p["skip"] and p["created"] <= window_end)]
    if not posts:
        return 0
    with db2.engine.begin() as conn:
        known = known_authorperms(conn, [p["authorperm"] for p in posts])
        blacklisted = blacklisted_members(conn, sorted({p["author"] for p in posts if not p["voted"]}))

    added = 0
    for post in posts:
        if post["authorperm"] in known:
            continue
        try:
            added += _qualify_post(db2, settings, load, sbi_voters, post, blacklisted, now)
        except Exception as exc:  # noqa: BLE001 - one post must not block the others
            print(f"{LOG}: qualifying {post['authorperm']} failed: {exc}")
    return added


def cast_votes(db2, magi, settings, broadcast_vote, now=None):
    """Vote on QUALIFIED posts, Hive-voted first, then oldest first. Returns broadcast attempts.

    broadcast_vote(json_body) signs and broadcasts the custom_json and returns
    nectar's transaction dict. It is only called with settings.broadcast on.
    """
    now = now or utcnow()
    with db2.engine.begin() as conn:
        expired = expire_candidates(conn, now - timedelta(days=CANDIDATE_DAYS), now)
        rows = vote_rows_with_status(conn, (QUALIFIED,))
        unsettled = in_flight_count(conn)
    if expired:
        print(f"{LOG}: {expired} candidate(s) expired without a vote")
    if not rows:
        return 0
    if unsettled:
        # The next dry run would check a meter that lacks the unsettled vote.
        print(f"{LOG}: {unsettled} earlier vote(s) not settled yet; no vote this run")
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
        try:
            # The window is recorded first, so every row counts against the
            # meter of its own window in later weight calculations.
            window = post_window(state.get(post_key(author, permlink)))

            if vote_key(author, permlink, settings.voter) in state:
                # A vote already exists (for example one cast on lassecash.com). A
                # second call would replace its weight and spend vote power again.
                with db2.engine.begin() as conn:
                    update_vote_row(conn, row["id"], now, status=SKIPPED, vote_window=window,
                                    error_message="voter already holds a vote on this post")
                continue

            if not row["hive_voted"]:
                with db2.engine.begin() as conn:
                    last = last_attempt(conn, author)
                if last is not None and now - last < LCASH_CAP:
                    note = f"L-cash cap: last vote attempt {last:%Y-%m-%d %H:%M} UTC"
                    if row["error_message"] != note:
                        with db2.engine.begin() as conn:
                            update_vote_row(conn, row["id"], now, error_message=note)
                    continue

            with db2.engine.begin() as conn:
                update_vote_row(conn, row["id"], now, vote_window=window)
                votes = meter_votes(conn, window, now - timedelta(days=LOOKBACK_DAYS)) + 1
            weight = compute_weight(window, votes)

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
        except MagiError as exc:
            # One row's Magi error leaves that row QUALIFIED for the next run.
            with db2.engine.begin() as conn:
                update_vote_row(conn, row["id"], now, error_message=f"Magi error: {exc}")
            print(f"{LOG}: {row['authorperm']} Magi error: {exc}")
            continue

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
                  f"(rc_limit {rc_limit}, window {window}, {votes} votes in lookback)")
            continue

        # Write-ahead: the row is PENDING, with the attempt time, before the
        # one step that cannot be rolled back. broadcast_at also starts the
        # author's L-cash cap.
        with db2.engine.begin() as conn:
            update_vote_row(conn, row["id"], now, status=PENDING, weight=weight,
                            rc_limit=rc_limit, broadcast_at=now, error_message=None)
        cast += 1
        try:
            tx = broadcast_vote(body)
        except Exception as exc:
            error = str(exc)
            if never_reached_chain(error):
                # A provable rejection is no vote attempt: the post returns to
                # the queue and the author's cap does not start.
                with db2.engine.begin() as conn:
                    update_vote_row(conn, row["id"], now, status=QUALIFIED,
                                    broadcast_at=None, error_message=error)
            else:
                # Any other error may hide a landed broadcast. The row stays
                # PENDING until settle_votes finds the call or proves it absent.
                with db2.engine.begin() as conn:
                    update_vote_row(conn, row["id"], now, status=PENDING, error_message=error)
            print(f"{LOG}: broadcast failed for {row['authorperm']}: {error}")
            continue
        trx_id = issue_trx_id(tx or {})
        with db2.engine.begin() as conn:
            update_vote_row(conn, row["id"], now, status=BROADCAST,
                            trx_id=None if trx_id == "N/A" else trx_id)
        print(f"{LOG}: voted {row['authorperm']} at {weight}% (rc_limit {rc_limit}, trx {trx_id})")
    return cast


def backfill_baseline(db, db2, magi, settings, load, sbi_voters, head_block, now=None):
    """Seed the weight lookback with the last LOOKBACK_DAYS of posts. Never signs.

    This replaces the shadow week. Each root post of an eligible staker from
    LOOKBACK_DAYS ago up to the candidate window replays through the
    candidate rules and the cap (apply_cap): a Hive-voted post counts, a post
    without the Hive vote counts when the cap allows. Counted tagged posts
    become BASELINE rows dated at the time the live job would have voted
    them; capped ones become SKIPPED rows. The replay does not defer a capped
    post as the live job does, so it can count fewer votes than the live job
    casts. compute_weight returns 100 for up to 42 votes, so the difference
    changes no weight at the 30-day volume of 2026-10.
    Posts in the candidate window are left to the live job. A post that
    already has a row is left alone, so a second backfill adds nothing twice.

    Returns a report that compares the tagged and untagged posts with the
    posts the contract registered since head_block - LOOKBACK_DAYS.
    """
    now = now or utcnow()
    since = now - timedelta(days=LOOKBACK_DAYS)
    until = now - timedelta(days=CANDIDATE_DAYS)
    with db2.engine.begin() as conn:
        authors = eligible_stakers(conn)
    report = {"stakers": len(authors), "posts": 0, "tagged": 0, "baseline": 0, "capped": 0,
              "untagged": 0, "unloaded": [], "tagged_registered": [],
              "tagged_not_registered": [], "untagged_registered": []}
    if not authors:
        return report
    with db.engine.begin() as conn:
        posts = member_root_posts(conn, authors, since, until)
    with db2.engine.begin() as conn:
        known = known_authorperms(conn, [p["authorperm"] for p in posts])
        blacklisted = blacklisted_members(conn, sorted({p["author"] for p in posts}))

    tagged, untagged = [], []
    for post in posts:
        if post["authorperm"] in known:
            continue
        if not post["voted"] and (post["skip"] or post["author"] in blacklisted):
            continue
        comment = load(post["authorperm"])
        if comment is None:
            report["unloaded"].append(post["authorperm"])
            continue
        _, permlink = resolve_authorperm(post["authorperm"])
        entry = dict(post, permlink=permlink,
                     hive_voted=post["voted"] or sbi_voted(comment, sbi_voters))
        if comment.is_main_post() and has_lassecash_tag(comment):
            tagged.append(entry)
        else:
            untagged.append(entry)

    counted, capped = apply_cap(tagged)
    state = magi.get_state(settings.contract_id,
                           [post_key(p["author"], p["permlink"]) for p in counted])
    with db2.engine.begin() as conn:
        for p in counted:
            insert_vote_row(conn, p["authorperm"], p["author"], p["permlink"], p["created"],
                            settings.voter, BASELINE, now, hive_voted=p["hive_voted"],
                            vote_window=post_window(state.get(post_key(p["author"], p["permlink"]))),
                            created_at=baseline_vote_time(p))
        for p in capped:
            insert_vote_row(conn, p["authorperm"], p["author"], p["permlink"], p["created"],
                            settings.voter, SKIPPED, now, hive_voted=p["hive_voted"],
                            error_message="baseline: L-cash cap")

    calls = magi.contract_vote_calls(settings.contract_id,
                                     max(0, head_block - LOOKBACK_DAYS * BLOCKS_PER_DAY))
    registered = {(c["author"], c["permlink"]) for c in calls if c["status"] == "CONFIRMED"}
    tagged_keys = {(qualify(p["author"]), p["permlink"]) for p in tagged}
    untagged_keys = {(qualify(p["author"]), p["permlink"]) for p in untagged}
    report.update(
        posts=len(posts), tagged=len(tagged), baseline=len(counted), capped=len(capped),
        untagged=len(untagged),
        tagged_registered=sorted(tagged_keys & registered),
        tagged_not_registered=sorted(tagged_keys - registered),
        untagged_registered=sorted(untagged_keys & registered),
    )
    return report


def print_backfill_report(report):
    print(f"{LOG}: backfill: {report['stakers']} eligible stakers, {report['posts']} root posts, "
          f"{report['tagged']} tagged ({report['baseline']} BASELINE, {report['capped']} capped), "
          f"{report['untagged']} untagged, {len(report['unloaded'])} not loaded")
    sections = (
        ("tagged and registered on the contract", "tagged_registered"),
        ("tagged, not registered (no LasseCash vote yet)", "tagged_not_registered"),
        ("registered but untagged: check the tag rule", "untagged_registered"),
    )
    for title, key in sections:
        print(f"{LOG}: {title}: {len(report[key])}")
        for author, permlink in report[key]:
            print(f"    {author}/{permlink}")
    for authorperm in report["unloaded"]:
        print(f"{LOG}: not loaded, rerun the backfill later: {authorperm}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="LasseCash companion vote")
    parser.add_argument("--backfill", action="store_true",
                        help="seed the 30-day weight lookback once, at deploy; never signs")
    return parser.parse_args(argv)


def run(argv=None):
    args = parse_args(argv)
    start = time.time()
    rt = get_runtime()
    cfg = rt["cfg"]
    settings = LassecashSettings.from_config(cfg)
    if not settings.enabled:
        print(f"{LOG}: disabled (config.json lassecash.enabled)")
        return

    sign = settings.broadcast and not args.backfill
    keys = []
    if sign:
        key_row = rt["storages"]["keys"].get(settings.voter, "posting")
        if key_row is None:
            print(f"{LOG}: no posting key stored for {settings.voter}; not voting")
            return
        keys.append(key_row["wif"].replace("\n", "").replace("\r", ""))
    hv = make_hive(cfg, keys=keys, num_retries=5, call_num_retries=3, timeout=15,
                   nobroadcast=not sign, expiration=TX_EXPIRATION_S)
    magi = MagiClient(settings.magi_nodes)
    sbi_voters = rt["accounts"]

    def load(authorperm):
        return load_comment(authorperm, hv)

    if args.backfill:
        members = rt["storages"]["member"].get_all_accounts()
        sync_lassecash_stakers(rt["db2"], magi, settings, members, 0)
        head_block = hv.get_dynamic_global_properties()["head_block_number"]
        report = backfill_baseline(rt["db"], rt["db2"], magi, settings, load, sbi_voters, head_block)
        print_backfill_report(report)
        print(f"{LOG}: backfill took {time.time() - start:.2f} s")
        return

    def broadcast_vote(body):
        return hv.custom_json(CUSTOM_JSON_ID, body, required_posting_auths=[settings.voter])

    def find(row):
        return find_broadcast(hv, settings, row)

    phases = (
        ("settle", lambda: settle_votes(rt["db2"], magi, settings, find)),
        ("qualify", lambda: qualify_posts(rt["db"], rt["db2"], settings, load, sbi_voters)),
        ("cast", lambda: cast_votes(rt["db2"], magi, settings, broadcast_vote)),
    )
    for name, phase in phases:
        try:
            phase()
        except Exception as exc:  # noqa: BLE001 - a failed phase must not skip the next
            print(f"{LOG}: {name} failed: {exc}")
    print(f"{LOG}: run took {time.time() - start:.2f} s")


if __name__ == "__main__":
    run()
