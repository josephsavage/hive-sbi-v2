import json
import time
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from hivesbi.settings import get_runtime
from hivesbi.storage import ConfigurationDB
from hivesbi.utils import ensure_timezone_aware
from hivesbi.issuance_log import (
    PENDING_TRX_PLACEHOLDER,
    UNCAPTURED_TRX_PLACEHOLDER,
    complete_uncaptured_success,
    fail_pending_with_proven_non_inclusion,
    insert_pending_issuance,
    issue_trx_id,
    log_issuance,
    mark_issuance_failure,
    mark_issuance_success,
    record_broadcast_error,
    record_resolution_attempt,
    utcnow,
)
from hivesbi.issue import (
    DEFAULT_ISSUER_ACCOUNT,
    fetch_issues_to_recipient,
    get_tokenholders,
    get_default_token_issuer,
)


# Pacing against the 5-custom_json-per-block limit is enforced per broadcast in
# hivesbi.issue.throttle_broadcast. main() used to also sleep BATCH_SLEEP_TIME at
# each issuance-loop boundary; that was the loop-granular half of the same scheme
# and is now redundant, so it was removed rather than left adding 6s a cycle.
TOKEN_PRECISION = Decimal("0.001")
MANAGEMENT_RECIPIENT = "josephsavage"

# rationale is the durable, never-rewritten reason an issuance happened. It is the
# ONLY secure way to attribute an issuance to a category (e.g. the Management 10%
# cap), so it is always written by the code that initiates the issuance and is
# never inferred from the chain (the chain custom_json carries no rationale, and
# josephsavage receives ordinary PIK dividends every cycle just like any member).
MANAGEMENT_RATIONALE = "Management"
PIK_RATIONALE = "pik"
ABC_RATIONALE = "Pending Balance Conversion"
CHAIN_MATCH_WINDOW = timedelta(minutes=30)
# Fixed: the scan window never grows. A PENDING row older than this is not
# chased by widening the walk — it is resolved one at a time against Hive Engine
# (resolve_pending_beyond_scan), which costs one small request instead of tens of
# thousands of ops. PENDING resolution therefore does NOT depend on this value —
# the audit-only passes do ('N/A' completion and the unexplained-issuance
# warning), and those act on SUCCESS rows, which are only ever reachable here.
#
# DO NOT LOWER. The hard minimum is one full cycle plus the match window:
# share_cycle_min (144 min) + CHAIN_MATCH_WINDOW + MATCH_CLOCK_SKEW ~= 2.98h. Below
# that, a SUCCESS row logged with UNCAPTURED_TRX_PLACEHOLDER in the previous cycle
# falls outside the scan and keeps its placeholder forever. 6h leaves 5.42h after
# the 35-minute margin = ~2.26 cycles, so every uncaptured row gets two full
# reconciliation passes and a missed pass is not permanent coverage loss (5h gave
# only 1.84 cycles — one pass plus a partial). The cost is paid in the high-VP
# regime, where the cycle fires every ~15 min and the window is re-walked ~24x.
CHAIN_SCAN_LOOKBACK = timedelta(hours=6)
# The scan breaks at `reached_cutoff`, so these are a runaway guard, not a budget:
# an ordinary cycle exits after a few hundred ops no matter how high they are set.
# They only bind in the high-VP regime, where the cycle fires roughly every 15
# minutes instead of every 144. Measured peak (2026-07-14) was 1597 issuances in a
# 5h window at current membership; the scan window is 6h, so budget ~5x the peak
# for membership growth. Note this counts EVERY custom_json on the issuer account,
# including hsbi_liquidpools broadcasts that never reach token_issuance_log — the
# measured figure is a floor. Running below the real peak is no longer fatal: a
# truncated scan reports the span it did walk and rows inside it still resolve.
# It only shrinks how far back one pass can reach.
HISTORY_SCAN_LIMIT = 10000
MATCH_CLOCK_SKEW = timedelta(minutes=5)
# One Hive block. When the scan stops at its op budget it may have consumed only
# part of a block, so proven coverage starts one block after the oldest op seen.
SCAN_TRUNCATION_MARGIN = timedelta(seconds=3)
# Most per-row Hive Engine lookups one pass may spend. Rows are taken
# least-recently-attempted first, so a backlog drains over consecutive cycles
# instead of turning one pass into an unbounded run of network calls, and rows
# that cannot be settled rotate to the back rather than consuming the whole cap
# every cycle while newer rows never get a turn.
MAX_BEYOND_SCAN_LOOKUPS = 200
# Wall clock one beyond-scan pass may spend, whatever the row count. The row cap
# bounds requests, not time: a history node that hangs instead of erroring costs
# the full httpx timeout per row, and sbirunner.sh runs jobs back to back, so
# every minute spent here delays hsbi_upvote_post_comment and members lose
# curation. Rows not reached are picked up next cycle.
BEYOND_SCAN_TIME_BUDGET = timedelta(minutes=5)
# A PENDING row older than two 144-minute cycles means reconciliation is not
# converging. The row blocks its (recipient, rationale) in issue_balance_tokens and
# the symptom is silent — a skipped member raises no error, it just stops being
# paid — so it has to be announced.
STUCK_PENDING_AGE = timedelta(hours=6)


def token_decimal(value):
    return Decimal(str(value or "0"))


def floor_token_amount(value):
    return token_decimal(value).quantize(TOKEN_PRECISION, rounding=ROUND_DOWN)


def calculate_management_issue_amount(outstanding, management_issued):
    """Management gets 10% of total real circulating supply.

    `outstanding` is real supply only (tokens - virtual_tokens); virtual delegation
    tokens never originate permanent management issuance. The /0.90 divisor accounts
    for the new issuance itself growing the real supply on the next sync.
    """
    return floor_token_amount(
        ((Decimal("0.10") * token_decimal(outstanding)) - token_decimal(management_issued))
        / Decimal("0.90")
    )


# ---------------------------------------------------------------------------
# write-ahead issuance log helpers live in hivesbi.issuance_log (shared with
# hivesbi.parse_hist_op for Unit Conversion); they are re-exported above for
# callers and tests.
# ---------------------------------------------------------------------------
# chain-confirm reconciliation
# ---------------------------------------------------------------------------


def _unwrap_history_op(history_row):
    if not isinstance(history_row, dict):
        return {}, {}
    op_data = history_row.get("op")
    if isinstance(op_data, (list, tuple)) and len(op_data) == 2:
        op = dict(op_data[1])
        op["type"] = op_data[0]
        return history_row, op
    if isinstance(op_data, dict):
        return history_row, op_data
    return history_row, history_row


def _parse_engine_issue(history_row, token_symbol="HSBIDAO"):
    row, op = _unwrap_history_op(history_row)
    raw_json = op.get("json")
    if raw_json is None:
        return None
    try:
        payload = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("contractName") != "tokens":
        return None
    if payload.get("contractAction") != "issue":
        return None

    contract_payload = payload.get("contractPayload") or {}
    if not isinstance(contract_payload, dict):
        return None
    if str(contract_payload.get("symbol", "")).upper() != token_symbol:
        return None

    recipient = (
        contract_payload.get("to")
        or contract_payload.get("recipient")
        or contract_payload.get("account")
    )
    quantity = contract_payload.get("quantity") or contract_payload.get("amount")
    trx_id = (
        row.get("trx_id")
        or row.get("transaction_id")
        or row.get("transactionId")
        or op.get("trx_id")
    )
    if not trx_id or not recipient or quantity is None:
        return None
    return {
        "trx_id": str(trx_id),
        "recipient": recipient,
        "units": floor_token_amount(quantity),
        "timestamp": ensure_timezone_aware(row.get("timestamp") or op.get("timestamp")),
    }


def fetch_recent_chain_issuance_scan(
    issuer, token_symbol="HSBIDAO", limit=HISTORY_SCAN_LIMIT, lookback=CHAIN_SCAN_LOOKBACK
):
    scan_started = utcnow()
    cutoff = scan_started - lookback
    try:
        history = issuer.hive_account.history_reverse(only_ops=["custom_json"])
    except TypeError:
        try:
            history = issuer.hive_account.history_reverse()
        except Exception as exc:
            print(f"Unable to fetch issuer history for reconciliation: {exc}")
            return {"issuances": [], "covered_since": None, "covered_until": None, "complete": False}
    except Exception as exc:
        print(f"Unable to fetch issuer history for reconciliation: {exc}")
        return {"issuances": [], "covered_since": None, "covered_until": None, "complete": False}

    issuances = []
    scanned = 0
    reached_cutoff = False
    oldest_scanned = None
    for history_row in history:
        scanned += 1
        if scanned > limit:
            break
        row, op = _unwrap_history_op(history_row)
        timestamp = ensure_timezone_aware(row.get("timestamp") or op.get("timestamp"))
        if timestamp is not None:
            # history_reverse walks newest-first, so the last timestamp seen is
            # the oldest op actually inspected.
            oldest_scanned = timestamp
        if timestamp is not None and timestamp < cutoff:
            reached_cutoff = True
            break
        issue = _parse_engine_issue(history_row, token_symbol=token_symbol)
        if issue is not None:
            issuances.append(issue)

    # `scanned` is limit+1 only when the op budget broke the loop. That is what
    # `complete` reports, and it drives the operator alert below — it deliberately
    # does NOT license a claim of unbounded coverage.
    complete = reached_cutoff or scanned <= limit
    if reached_cutoff:
        covered_since = cutoff
    elif oldest_scanned is not None:
        # Truncated at the op budget before reaching `cutoff`. Coverage is
        # partial, not absent: every op between `oldest_scanned` and the account
        # head was inspected, so absence is still proven over that span. Reporting
        # None here instead is what let one unreachable PENDING row disable
        # failure-resolution for every other row (see reconcile_recent_issuances).
        #
        # The walk may have stopped mid-block, so ops sharing `oldest_scanned`
        # may be unseen; claim coverage from the first instant provably past it.
        #
        # This is also where a generator that simply ran out lands. It used to
        # claim datetime.min — total coverage of all time — on the theory that a
        # history which ended was walked in full. But an exhausted iterator and
        # one that stopped early are indistinguishable from here, and
        # HISTORY_SCAN_LIMIT is exactly 10x nectar's 1000-op history_reverse
        # batch, so a batch-boundary stop lands here by construction. Claiming
        # datetime.min there would fail every PENDING row older than the match
        # window in a single pass, and a wrongly failed row is re-issued next
        # cycle: HSBIDAO minted twice, irreversibly. Under-claiming only sends
        # older rows to the per-row lookup, which is what it exists for.
        covered_since = oldest_scanned + SCAN_TRUNCATION_MARGIN
    else:
        covered_since = None
    # history_reverse starts at the account head, so a successful fetch covers
    # everything up to the present: no op newer than the newest scanned op can
    # exist. The skew margin absorbs API indexing lag and chain-vs-app clock
    # drift, so an issuer with no recent activity can still prove absence.
    covered_until = scan_started - MATCH_CLOCK_SKEW
    return {
        "issuances": issuances,
        "covered_since": covered_since,
        "covered_until": covered_until,
        "complete": complete,
    }


def fetch_recent_chain_issuances(issuer, token_symbol="HSBIDAO", limit=HISTORY_SCAN_LIMIT):
    """Backward-compatible helper returning only issuances from the scan."""
    return fetch_recent_chain_issuance_scan(issuer, token_symbol, limit)["issuances"]


def _chain_issue_matches_pending(chain_issue, pending_row):
    if chain_issue["recipient"] != pending_row["recipient"]:
        return False
    if token_decimal(chain_issue["units"]) != pending_row["units"]:
        return False

    chain_timestamp = chain_issue.get("timestamp")
    issued_at = pending_row.get("issued_at")
    if chain_timestamp is None or issued_at is None:
        return False
    return issued_at - MATCH_CLOCK_SKEW <= chain_timestamp <= issued_at + CHAIN_MATCH_WINDOW


def _chain_issue_matches_logged_success(chain_issue, success_row):
    if chain_issue["recipient"] != success_row["recipient"]:
        return False
    if token_decimal(chain_issue["units"]) != success_row["units"]:
        return False

    chain_timestamp = chain_issue.get("timestamp")
    issued_at = success_row.get("issued_at")
    if chain_timestamp is None or issued_at is None:
        return False
    return issued_at - MATCH_CLOCK_SKEW <= chain_timestamp <= issued_at + CHAIN_MATCH_WINDOW


def _chain_sort_key(chain_issue):
    return chain_issue.get("timestamp") or datetime.max.replace(tzinfo=timezone.utc)


def _issued_at_of(row):
    return row["issued_at"]


def _issue_timestamp_of(issue):
    return issue["timestamp"]


def _rank_by_proximity(items, target, timestamp_of):
    """Order `items` by how close their timestamp sits to `target`, nearest first.

    Hive Engine issuance does not carry our rationale and the nectarengine issue
    helper cannot attach a memo, so when a recipient receives the same amount for
    two rationales minutes apart, timestamp proximity is the only durable
    discriminator either matching path has. Both use it through here.
    """
    return sorted(items, key=lambda item: abs(timestamp_of(item) - target))


def _proximity_is_tied(ranked, target, timestamp_of):
    """True when the two closest candidates are exactly equidistant from `target`."""
    if len(ranked) < 2:
        return False
    return abs(timestamp_of(ranked[0]) - target) == abs(timestamp_of(ranked[1]) - target)


def _match_pending_rows_to_chain(pending_rows, chain_issuances):
    """Match on-chain issues to the closest compatible PENDING row.

    Issue-first: each chain issue claims the nearest row that could have produced
    it. resolve_pending_beyond_scan walks the same decision row-first, because it
    fetches evidence one row at a time; both rank with _rank_by_proximity.
    """
    matches = {}
    used_log_ids = set()
    used_trx_ids = set()

    for chain_issue in sorted(chain_issuances, key=_chain_sort_key):
        candidates = [
            row
            for row in pending_rows
            if row["id"] not in used_log_ids
            and _chain_issue_matches_pending(chain_issue, row)
        ]
        if not candidates:
            continue

        chain_timestamp = chain_issue.get("timestamp")
        if chain_timestamp is None:
            continue
        ranked = _rank_by_proximity(candidates, chain_timestamp, _issued_at_of)
        # A tie here is safe to defer: the row stays contested rather than
        # failed, and once its sibling records the chain trx_id the next pass
        # excludes that issue and the tie is gone.
        if _proximity_is_tied(ranked, chain_timestamp, _issued_at_of):
            continue
        best = ranked[0]
        matches[best["id"]] = chain_issue
        used_log_ids.add(best["id"])
        used_trx_ids.add(chain_issue["trx_id"])
    return matches, used_trx_ids


def _find_logged_success_for_chain_issue(conn, chain_issue, trx_id=None):
    params = [
        chain_issue["recipient"],
        token_decimal(chain_issue["units"]),
        PENDING_TRX_PLACEHOLDER,
    ]
    trx_filter = ""
    if trx_id is not None:
        trx_filter = "AND trx_id = %s"
        params.append(trx_id)

    rows = conn.exec_driver_sql(
        f"""
        SELECT id, recipient, units, rationale, issued_at
        FROM token_issuance_log
        WHERE status = 'SUCCESS'
          AND recipient = %s
          AND units = %s
          AND trx_id <> %s
          {trx_filter}
        ORDER BY issued_at DESC
        """,
        tuple(params),
    ).fetchall()

    candidates = [
        {
            "id": row[0],
            "recipient": row[1],
            "units": token_decimal(row[2]),
            "rationale": row[3],
            "issued_at": ensure_timezone_aware(row[4]),
        }
        for row in rows
    ]
    matches = [
        row
        for row in candidates
        if _chain_issue_matches_logged_success(chain_issue, row)
    ]
    if not matches:
        return None

    chain_timestamp = chain_issue.get("timestamp")
    if chain_timestamp is None:
        return matches[0]
    return min(
        matches,
        key=lambda row: abs(chain_timestamp - row["issued_at"])
        if row.get("issued_at") is not None
        else CHAIN_MATCH_WINDOW,
    )


def _complete_balance_issuance_effect(conn, pending_row):
    rationale = pending_row["rationale"]
    if rationale == PIK_RATIONALE:
        balance_column = "pik"
    elif rationale == ABC_RATIONALE:
        balance_column = "abc_pik"
    else:
        return

    conn.exec_driver_sql(
        f"""
        UPDATE tokenholders
        SET {balance_column} = GREATEST({balance_column} - %s, 0)
        WHERE member_name = %s
        """,
        (pending_row["units"], pending_row["recipient"]),
    )


def reconcile_issuances(
    conn,
    chain_issuances,
    covered_since=None,
    covered_until=None,
):
    """Resolve write-ahead PENDING rows against recent on-chain issuances.

    Reconciliation uses the blockchain timestamp as the authority. A PENDING row
    is confirmed only by a matching chain issue (recipient + units + chain
    timestamp near the intent timestamp). A PENDING row is failed only when the
    chain scan provably covered its entire possible match window AND no in-window
    chain issue could plausibly be its broadcast (a contested row stays PENDING;
    once the sibling SUCCESS row records the chain trx_id, the next pass excludes
    that issue and the row resolves).

    Coverage is per-row and expressed entirely by the [covered_since,
    covered_until] interval; there is deliberately no scan-wide "complete" flag.
    A truncated scan still proves absence over the span it walked, and gating on
    scan-wide completeness meant a single row older than the scan could reach
    blocked every other row from ever resolving. Callers that cannot prove any
    coverage pass covered_since=None, which fails the check for every row.

    rationale is read from the existing PENDING row and is never inferred from the
    chain or rewritten.
    """
    logged_trx = {
        r[0]
        for r in conn.exec_driver_sql(
            "SELECT trx_id FROM token_issuance_log WHERE trx_id <> %s",
            (PENDING_TRX_PLACEHOLDER,),
        ).fetchall()
    }
    unmatched_chain = [c for c in chain_issuances if c["trx_id"] not in logged_trx]

    # Pass 1 — adopt chain issues that provably belong to an existing SUCCESS row
    # whose broadcast returned no trx_id (logged UNCAPTURED_TRX_PLACEHOLDER).
    #
    # This MUST run before pending matching. A chain issue left in the pool here
    # can be handed to an unrelated PENDING row of the same recipient and amount,
    # stamping that row with a trx_id belonging to a different issuance while the
    # real uncaptured row keeps its placeholder forever.
    completed_trx_ids = set()
    completed_log_ids = set()
    for chain_issue in sorted(unmatched_chain, key=_chain_sort_key):
        uncaptured = _find_logged_success_for_chain_issue(
            conn, chain_issue, trx_id=UNCAPTURED_TRX_PLACEHOLDER
        )
        # The UPDATE below clears the placeholder, so a completed row cannot be
        # re-selected; completed_log_ids guards the same invariant explicitly.
        if uncaptured is None or uncaptured["id"] in completed_log_ids:
            continue
        complete_uncaptured_success(conn, uncaptured["id"], chain_issue["trx_id"])
        completed_log_ids.add(uncaptured["id"])
        completed_trx_ids.add(chain_issue["trx_id"])
        print(
            "Completed logged issuance from chain: "
            f"{chain_issue['trx_id']} {chain_issue['recipient']} "
            f"{chain_issue['units']} {uncaptured['rationale']}"
        )

    unmatched_chain = [
        c for c in unmatched_chain if c["trx_id"] not in completed_trx_ids
    ]

    # Pass 2 — resolve write-ahead PENDING rows against what is left.
    pending_rows = conn.exec_driver_sql(
        """
        SELECT id, recipient, units, rationale, issued_at
        FROM token_issuance_log
        WHERE status = 'PENDING'
        ORDER BY issued_at ASC
        """
    ).fetchall()

    pending = [
        {
            "id": row[0],
            "recipient": row[1],
            "units": token_decimal(row[2]),
            "rationale": row[3],
            "issued_at": ensure_timezone_aware(row[4]),
        }
        for row in pending_rows
    ]
    matches, matched_trx_ids = _match_pending_rows_to_chain(pending, unmatched_chain)

    # A pending row with any in-window chain candidate is contested: that issue
    # may be its own broadcast (tie-skipped, or attributed to a sibling row by
    # timestamp proximity), so absence is not proven and the row must not be
    # failed this pass.
    contested = {
        row["id"]
        for row in pending
        if row["id"] not in matches
        and any(
            _chain_issue_matches_pending(chain_issue, row)
            for chain_issue in unmatched_chain
        )
    }

    for pending_row in pending:
        log_id = pending_row["id"]
        recipient = pending_row["recipient"]
        units = pending_row["units"]
        rationale = pending_row["rationale"]
        match = matches.get(log_id)
        if match is not None:
            mark_issuance_success(conn, log_id, match["trx_id"])
            _complete_balance_issuance_effect(conn, pending_row)
            print(
                f"Reconciled PENDING -> SUCCESS: {recipient} {units} "
                f"{rationale} ({match['trx_id']})"
            )
            continue

        issued_at_aware = pending_row["issued_at"]
        scan_covers_row = (
            covered_since is not None
            and covered_until is not None
            and issued_at_aware is not None
            and covered_since <= issued_at_aware - MATCH_CLOCK_SKEW
            and covered_until >= issued_at_aware + CHAIN_MATCH_WINDOW
        )
        if scan_covers_row and log_id in contested:
            print(
                f"Leaving contested PENDING unresolved: {recipient} {units} {rationale}"
            )
            continue
        if scan_covers_row:
            mark_issuance_failure(
                conn,
                log_id,
                "No matching on-chain issuance within chain match window",
            )
            print(f"Reconciled PENDING -> FAILURE: {recipient} {units} {rationale}")

    # Pass 3 — anything still unclaimed is either already explained by a logged
    # SUCCESS row (nothing to do) or has no origin in the log at all (report only;
    # never insert, or the log double-counts the issuance — see CHANGES.md, #138).
    unmatched_chain = [
        c for c in unmatched_chain if c["trx_id"] not in matched_trx_ids
    ]
    for chain_issue in unmatched_chain:
        explained = _find_logged_success_for_chain_issue(conn, chain_issue)
        if explained is not None:
            print(
                "Recognized already-logged on-chain issuance: "
                f"{chain_issue['trx_id']} {chain_issue['recipient']} "
                f"{chain_issue['units']} {explained['rationale']}"
            )
            continue

        print(
            "Unexplained on-chain issuance without token_issuance_log origin: "
            f"{chain_issue['trx_id']} {chain_issue['recipient']} {chain_issue['units']}"
        )


def resolve_pending_beyond_scan(
    db2,
    covered_since,
    issuer_account=DEFAULT_ISSUER_ACCOUNT,
    token_symbol=None,
    time_budget=BEYOND_SCAN_TIME_BUDGET,
):
    """Resolve PENDING rows the bulk chain scan does not reach back far enough for.

    The bulk scan walks the issuer's whole custom_json stream, so its reach is
    bounded by op volume, and the window is deliberately fixed — a row older than
    CHAIN_SCAN_LOOKBACK is never chased by widening that walk. This asks Hive
    Engine directly whether that one issuance exists, scoped to the recipient, the
    symbol and the row's own 35-minute match window, so the cost is one small
    request per stuck row no matter how old it is.

    Reaching this path at all means the row carries no proof of its own fate:
    fail_pending_with_proven_non_inclusion has already failed everything whose
    recorded error shows it never reached the chain, so what is left is a timeout,
    a transport failure, or a process that died between the write-ahead insert and
    either outcome write. Only the chain can settle those.

    Lookups run outside any open transaction — each may block for the httpx
    timeout, and holding a write transaction on token_issuance_log for that long
    stalls every other reader of the table.

    Two limits bound one pass: MAX_BEYOND_SCAN_LOOKUPS rows, and `time_budget` of
    wall clock. The row cap alone does not bound duration — a history node that
    hangs rather than errors turns the cap into cap x timeout, and sbirunner.sh is
    sequential, so that time comes straight out of the voting window of every job
    downstream. Rows not reached are taken next cycle.

    Selection rotates by last_resolution_attempt (never-attempted first) so a
    block of unresolvable rows cannot starve the rows behind it.

    Uncertainty leaves the row PENDING: an untrusted lookup, an unelapsed match
    window, an issue outside the row's own window, or an uncaptured sibling with a
    claim on it. Two candidates do NOT leave it pending — see below.
    """
    if covered_since is None:
        # The scan proved no coverage at all (its own history fetch failed), so
        # every PENDING row is beyond its reach. The per-row lookup does not
        # depend on that scan, so it still settles them; returning here instead
        # would let one failing API disable resolution for everyone.
        horizon = utcnow()
    else:
        horizon = covered_since + MATCH_CLOCK_SKEW
    # The match window must also have fully elapsed, exactly as for the bulk scan.
    settled_before = utcnow() - MATCH_CLOCK_SKEW - CHAIN_MATCH_WINDOW

    with db2.engine.begin() as conn:
        rows = conn.exec_driver_sql(
            """
            SELECT id, recipient, units, rationale, issued_at
            FROM token_issuance_log
            WHERE status = 'PENDING' AND issued_at < LEAST(%s, %s)
            ORDER BY last_resolution_attempt ASC, issued_at ASC
            LIMIT %s
            """,
            (horizon, settled_before, MAX_BEYOND_SCAN_LOOKUPS),
        ).fetchall()

    deadline = time.monotonic() + time_budget.total_seconds()
    attempted = 0

    for row in rows:
        if time.monotonic() >= deadline:
            print(
                f"Beyond-scan resolution hit its time budget ({time_budget}) after "
                f"{attempted} of {len(rows)} row(s); the rest are taken next cycle."
            )
            break

        log_id, recipient, rationale = row[0], row[1], row[3]
        units = token_decimal(row[2])
        issued_at = ensure_timezone_aware(row[4])
        if issued_at is None:
            # issued_at is NOT NULL in schema, so this is defensive only — but an
            # unstamped row keeps its place at the head of the queue forever, so
            # stamp before skipping rather than let one bad row starve the rest.
            with db2.engine.begin() as conn:
                record_resolution_attempt(conn, log_id)
            continue

        window_start = issued_at - MATCH_CLOCK_SKEW
        window_end = issued_at + CHAIN_MATCH_WINDOW
        issues = fetch_issues_to_recipient(
            recipient,
            window_start,
            window_end,
            symbol=token_symbol,
            issuer_account=issuer_account,
        )
        attempted += 1

        with db2.engine.begin() as conn:
            # Stamped whatever happens below, so a row that cannot be settled goes
            # to the back of the queue instead of consuming a lookup every cycle
            # forever while newer rows never get one.
            record_resolution_attempt(conn, log_id)

            if issues is None:
                print(
                    f"Hive Engine lookup unavailable for stuck PENDING id={log_id} "
                    f"{recipient} {units} {rationale}; leaving PENDING"
                )
                continue

            # A SUCCESS row for the same recipient and amount whose broadcast
            # returned no trx_id has an unidentified claim on one of these issues.
            # Pass 1 of reconcile_issuances resolves that, but it only sees rows
            # inside the scan, so here the claim cannot be told apart from ours —
            # leave both alone.
            #
            # The bound is the sibling's own match window, not ours: a sibling
            # claims any issue in [its issued_at - MATCH_CLOCK_SKEW, its issued_at
            # + CHAIN_MATCH_WINDOW], so its claim overlaps ours whenever its
            # issued_at falls within CHAIN_MATCH_WINDOW + MATCH_CLOCK_SKEW of ours.
            # Scoping to CHAIN_MATCH_WINDOW alone left a blind spot at each end in
            # which a shared issue was handed to this row, stamping it with the
            # sibling's trx_id and debiting a balance for tokens never issued
            # against it.
            sibling_reach = CHAIN_MATCH_WINDOW + MATCH_CLOCK_SKEW
            uncaptured_sibling = conn.exec_driver_sql(
                """
                SELECT COUNT(*) FROM token_issuance_log
                WHERE status = 'SUCCESS' AND recipient = %s AND units = %s
                  AND trx_id = %s AND issued_at BETWEEN %s AND %s
                """,
                (
                    recipient,
                    units,
                    UNCAPTURED_TRX_PLACEHOLDER,
                    issued_at - sibling_reach,
                    issued_at + sibling_reach,
                ),
            ).scalar()
            if uncaptured_sibling:
                print(
                    f"Uncaptured sibling issuance claims the same window for stuck "
                    f"PENDING id={log_id} {recipient} {units} {rationale}; "
                    "leaving PENDING"
                )
                continue

            # Exclude issues already claimed by another log row, so a recipient who
            # legitimately received the same amount twice resolves one row per issue.
            logged_trx = {
                r[0]
                for r in conn.exec_driver_sql(
                    "SELECT trx_id FROM token_issuance_log WHERE recipient = %s "
                    "AND trx_id <> %s",
                    (recipient, PENDING_TRX_PLACEHOLDER),
                ).fetchall()
            }
            # The window is re-checked locally rather than trusted to the remote
            # timestampStart/timestampEnd: the history endpoint is chosen from a
            # beacon at runtime, and a node that silently ignored those params
            # would hand back the recipient's most recent issues instead. Matching
            # one of those would mark this row SUCCESS and debit a balance against
            # an issuance that was never made for it.
            candidates = [
                issue
                for issue in issues
                if floor_token_amount(issue["quantity"]) == units
                and issue["trx_id"] not in logged_trx
                and issue["timestamp"] is not None
                and window_start <= issue["timestamp"] <= window_end
            ]

            if not candidates:
                mark_issuance_failure(
                    conn,
                    log_id,
                    "No matching Hive Engine issuance in match window",
                )
                print(
                    f"Resolved stuck PENDING -> FAILURE from Hive Engine: {recipient} "
                    f"{units} {rationale}"
                )
                continue

            # More than one candidate is resolved here, not deferred. Deferring is
            # what the bulk matcher does, and it is right there because a contested
            # row resolves on a later pass once its sibling records a trx_id. Here
            # nothing else ever moves these rows: two PENDING rows for the same
            # recipient and amount with overlapping windows would each see both
            # issues, each defer, and deadlock as PENDING forever — the "member
            # silently stops being paid" symptom this whole path exists to cure.
            # Every candidate carries the same units by construction, so a swapped
            # attribution costs trx_id accuracy in the audit trail, never tokens:
            # whichever row loses its issue finds no candidate next pass, fails,
            # and re-issues the identical amount.
            ranked = _rank_by_proximity(candidates, issued_at, _issue_timestamp_of)
            if _proximity_is_tied(ranked, issued_at, _issue_timestamp_of):
                # Equidistant, so proximity cannot choose. Order by trx_id: it is
                # stable across passes, which keeps the choice reproducible when an
                # operator audits it later.
                ranked = sorted(ranked, key=lambda issue: issue["trx_id"])
                print(
                    f"Ambiguous Hive Engine match for stuck PENDING id={log_id} "
                    f"{recipient} {units} {rationale}; taking {ranked[0]['trx_id']} "
                    "by trx_id order"
                )
            chosen = ranked[0]
            pending_row = {
                "id": log_id,
                "recipient": recipient,
                "units": units,
                "rationale": rationale,
                "issued_at": issued_at,
            }
            mark_issuance_success(conn, log_id, chosen["trx_id"])
            _complete_balance_issuance_effect(conn, pending_row)
            print(
                f"Resolved stuck PENDING -> SUCCESS from Hive Engine: {recipient} "
                f"{units} {rationale} ({chosen['trx_id']})"
            )

    if len(rows) == MAX_BEYOND_SCAN_LOOKUPS and attempted == len(rows):
        print(
            f"Beyond-scan resolution hit its per-pass cap "
            f"({MAX_BEYOND_SCAN_LOOKUPS}); remaining rows are taken next cycle."
        )


def warn_stuck_pending(conn):
    """Announce PENDING rows that reconciliation should already have resolved.

    issue_balance_tokens skips any member holding a live PENDING row for the same
    rationale, so an unresolvable row silently stops that member's dividends.

    A truncated chain scan is no longer a cause: it reports the span it did walk
    and rows inside it still resolve, while rows outside it go to the per-row
    Hive Engine lookup. What is left is an unreachable history API (every lookup
    returning unknown), an uncaptured sibling holding a claim on the same window,
    or a backlog deeper than one pass's budget.
    """
    rows = conn.exec_driver_sql(
        """
        SELECT id, recipient, units, rationale, issued_at, error_message
        FROM token_issuance_log
        WHERE status = 'PENDING' AND issued_at < %s
        ORDER BY issued_at ASC
        """,
        (utcnow() - STUCK_PENDING_AGE,),
    ).fetchall()
    for row in rows:
        print(
            f"ALERT: stuck PENDING issuance id={row[0]} recipient={row[1]} "
            f"units={row[2]} rationale={row[3]} issued_at={row[4]} error={row[5]}"
        )
    return len(rows)


def reconcile_recent_issuances(db2, issuer):
    # Free first: a row whose recorded broadcast error already proves the
    # operation never reached the chain needs no chain evidence at all. This is
    # one UPDATE and no network, and it clears the whole class of row that used
    # to force the scan window open.
    with db2.engine.begin() as conn:
        swept = fail_pending_with_proven_non_inclusion(conn)
    if swept:
        print(
            f"Failed {swept} PENDING issuance(s) whose recorded error proves the "
            "broadcast never reached the chain; they re-issue next cycle."
        )

    # The window is fixed. It used to be widened to cover the oldest PENDING row,
    # which pinned the op budget at its ceiling on every cycle for as long as one
    # unresolvable row existed — tens of thousands of ops re-walked per pass to
    # rediscover a single row's fate. Rows outside this window go to the per-row
    # Hive Engine lookup below, which costs one small request each.
    # The symbol travels with the issuer, exactly as the account name does.
    # Defaulting it here instead would check a non-HSBIDAO issuer's rows against
    # HSBIDAO history, find nothing, and fail every one of them.
    token_symbol = issuer.token_symbol
    scan = fetch_recent_chain_issuance_scan(
        issuer,
        token_symbol=token_symbol,
        lookback=CHAIN_SCAN_LOOKBACK,
        limit=HISTORY_SCAN_LIMIT,
    )
    if not scan["complete"]:
        print(
            "ALERT: chain issuance scan truncated at its op budget "
            f"(lookback={CHAIN_SCAN_LOOKBACK}, limit={HISTORY_SCAN_LIMIT}); "
            f"coverage starts at {scan['covered_since']}. Rows inside that span "
            "still resolve; older ones fall through to the per-row Hive Engine "
            "lookup. Raise HISTORY_SCAN_LIMIT if this persists."
        )

    with db2.engine.begin() as conn:
        reconcile_issuances(
            conn,
            scan["issuances"],
            covered_since=scan["covered_since"],
            covered_until=scan["covered_until"],
        )

    # Outside the transaction above: each lookup can block for the httpx timeout.
    resolve_pending_beyond_scan(
        db2,
        scan["covered_since"],
        issuer_account=issuer.account_name,
        token_symbol=token_symbol,
    )

    with db2.engine.begin() as conn:
        warn_stuck_pending(conn)


# ---------------------------------------------------------------------------
# issuance paths
# ---------------------------------------------------------------------------


def issue_balance_tokens(db2, issuer, rationale, balance_column):
    """Write-ahead issuance for per-member balances (pik / abc_pik).

    Each member with a positive balance gets a committed PENDING intent before
    broadcast. If the process dies after broadcast and before SUCCESS is recorded,
    reconciliation completes the same row from chain history.
    """
    with db2.engine.begin() as conn:
        balance_rows = conn.exec_driver_sql(
            f"SELECT member_name, {balance_column} FROM tokenholders "
            f"WHERE {balance_column} > 0"
        ).fetchall()

    for row in balance_rows:
        member_name = row[0]
        amount = floor_token_amount(row[1])
        if amount < TOKEN_PRECISION:
            continue
        # Pacing is not done here: the 5-custom_json-per-block budget belongs to
        # the issuer account across every loop in the process, so it is enforced
        # inside TokenIssuer (hivesbi.issue.throttle_broadcast).

        with db2.engine.begin() as conn:
            pending_count = conn.exec_driver_sql(
                """
                SELECT COUNT(*) FROM token_issuance_log
                WHERE status = 'PENDING' AND recipient = %s AND rationale = %s
                """,
                (member_name, rationale),
            ).scalar()
            if pending_count:
                print(
                    f"Skipping {rationale} issuance to {member_name}: "
                    "existing PENDING intent"
                )
                continue
            log_id = insert_pending_issuance(conn, member_name, amount, rationale)

        print(f"Issuing {amount} HSBIDAO ({rationale}) to {member_name}")
        try:
            tx = issuer.issue(member_name, float(amount))
            trx_id = issue_trx_id(tx)
            print("Issued:", tx)
        except Exception as e:
            with db2.engine.begin() as conn:
                status = record_broadcast_error(conn, log_id, str(e))
            print(f"Failed to issue to {member_name} (row -> {status}): {e}")
            continue

        with db2.engine.begin() as conn:
            conn.exec_driver_sql(
                f"""
                UPDATE tokenholders
                SET {balance_column} = GREATEST({balance_column} - %s, 0)
                WHERE member_name = %s
                """,
                (amount, member_name),
            )
            mark_issuance_success(conn, log_id, trx_id)


def issue_management_tokens(db2, issuer):
    with db2.engine.begin() as conn:
        management_issued = token_decimal(
            conn.exec_driver_sql(
                """
                SELECT COALESCE(SUM(units), 0) AS units
                FROM token_issuance_log
                WHERE rationale = %s AND status IN ('SUCCESS', 'PENDING')
                """,
                (MANAGEMENT_RATIONALE,),
            ).scalar()
        )
        outstanding = token_decimal(
            conn.exec_driver_sql(
                """
                SELECT COALESCE(SUM(tokens - virtual_tokens), 0) AS tokens
                FROM tokenholders
                WHERE member_name <> 'sbi-tokens'
                """
            ).scalar()
        )

    issue_amount = calculate_management_issue_amount(outstanding, management_issued)
    if issue_amount < TOKEN_PRECISION:
        print(
            "Skipping Management issuance: "
            f"outstanding={outstanding} management_issued={management_issued}"
        )
        return

    # write-ahead: the PENDING row counts toward the cap immediately, so a crash
    # after broadcast cannot lead to a second issuance.
    with db2.engine.begin() as conn:
        log_id = insert_pending_issuance(
            conn, MANAGEMENT_RECIPIENT, issue_amount, MANAGEMENT_RATIONALE
        )

    print(f"Issuing {issue_amount} HSBIDAO Management tokens to {MANAGEMENT_RECIPIENT}")
    try:
        tx = issuer.issue(MANAGEMENT_RECIPIENT, float(issue_amount))
        trx_id = issue_trx_id(tx)
        print("Issued:", tx)
    except Exception as e:
        with db2.engine.begin() as conn:
            status = record_broadcast_error(conn, log_id, str(e))
        print(
            f"Failed Management issuance to {MANAGEMENT_RECIPIENT} "
            f"(row -> {status}): {e}"
        )
        return

    with db2.engine.begin() as conn:
        mark_issuance_success(conn, log_id, trx_id)


def sync_tokenholders(db2):
    holders = get_tokenholders()
    print("Upserting tokenholders into DB")
    with db2.engine.begin() as conn:
        conn.exec_driver_sql("UPDATE tokenholders SET liquid_tokens = 0")
        for h in holders:
            conn.exec_driver_sql(
                """
                INSERT INTO tokenholders (snapshot_timestamp, member_name, liquid_tokens)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    snapshot_timestamp = VALUES(snapshot_timestamp),
                    liquid_tokens = VALUES(liquid_tokens)
                """,
                (datetime.now(timezone.utc), h["account"], h["balance"]),
            )


def main():
    rt = get_runtime()

    # Open configuration database via storages
    stor = rt["storages"]
    confStorage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()
    share_cycle_min = conf_setup["share_cycle_min"]

    db2 = rt.get("db2")
    if db2 is not None:
        with db2.engine.begin() as conn:
            # get max mana_pct from accounts table
            result = conn.exec_driver_sql(
                "SELECT MAX(mana_pct) AS max_mana_pct FROM accounts"
            ).fetchone()

            max_mana_pct = (
                result.max_mana_pct or 0
            )  # or result.max_mana_pct if using RowMapping
            print("hsbi_token_snapshot fetching max VP level: ", max_mana_pct)

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
        issuer = get_default_token_issuer()

        # Resolve any unconfirmed PENDING issuances from a prior cycle before
        # issuing more (keeps the Management cap and member balances accurate).
        reconcile_recent_issuances(db2, issuer)

        # curation PIK token issuance
        issue_balance_tokens(db2, issuer, PIK_RATIONALE, "pik")

        # Pending Balance Conversion issuance
        issue_balance_tokens(db2, issuer, ABC_RATIONALE, "abc_pik")

        # Refresh liquid balances from Hive Engine, then issue the Management 10%
        # against real circulating supply only.
        sync_tokenholders(db2)
        issue_management_tokens(db2, issuer)


if __name__ == "__main__":
    main()
