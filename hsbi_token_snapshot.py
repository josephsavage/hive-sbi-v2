import time
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from hivesbi.settings import get_runtime
from hivesbi.storage import ConfigurationDB
from hivesbi.utils import ensure_timezone_aware
from hivesbi.issuance_log import (
    PENDING_TRX_PLACEHOLDER,
    UNCAPTURED_TRX_PLACEHOLDER,
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
# The per-row question this module asks the chain is "did recipient R receive
# exactly U HSBIDAO from us near time T?". CHAIN_MATCH_WINDOW is how long after
# the intent an issuance may still show up, MATCH_CLOCK_SKEW absorbs chain-vs-app
# clock drift and history-indexing lag on the other side.
CHAIN_MATCH_WINDOW = timedelta(minutes=30)
MATCH_CLOCK_SKEW = timedelta(minutes=5)
# Most Hive Engine lookups one pass may spend. Rows are taken
# least-recently-attempted first, so a backlog drains over consecutive cycles and
# rows that cannot be settled rotate to the back instead of consuming the whole
# cap every cycle while newer rows never get a turn.
MAX_RESOLUTION_LOOKUPS = 200
# Wall clock one resolution pass may spend, whatever the row count. The row cap
# bounds requests, not time: a history node that hangs instead of erroring costs
# the full httpx timeout per row, and sbirunner.sh runs jobs back to back, so
# every minute spent here delays hsbi_upvote_post_comment and members lose
# curation. Rows not reached are picked up next cycle.
RESOLUTION_TIME_BUDGET = timedelta(minutes=5)
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
# resolving unconfirmed issuances
#
# THE REMIT — read this before adding anything here.
#
# Four paths issue HSBIDAO, and each one already retries safely on its own. What
# they cannot survive is an issuance that reached the chain while the record that
# would stop it being repeated did not get written:
#
#   pik / Pending Balance Conversion — the ledger is tokenholders.pik / .abc_pik.
#       The balance is debited only in the same transaction that records SUCCESS,
#       so a broadcast that fails leaves the balance intact and next cycle simply
#       reissues it. The one unrecoverable outcome is issued-but-not-debited: the
#       same tokens are minted again next cycle, and an on-chain mint cannot be
#       undone.
#   Management — the ledger is this log: the 10% cap counts SUM(units) over
#       SUCCESS + PENDING rows. Issued-but-not-logged undercounts the cap and
#       management is overpaid next cycle.
#   Unit Conversion — see RESOLVABLE_RATIONALES below. Not resolved here.
#
# So the only question this module needs to answer, for one row at a time, is
# "did recipient R receive exactly U HSBIDAO from us in [T - skew, T + window]?".
# fetch_issues_to_recipient answers exactly that in one small request.
#
# It deliberately does NOT reconstruct which chain transaction belongs to which
# log row. That question has no economic content — every candidate carries the
# same units by construction, so a swapped trx_id costs audit-trail precision and
# never tokens — and answering it is where the damage came from: PR #138 walked
# the issuer's whole custom_json history, matched it against the log in bulk, and
# duplicated every Unit Conversion row under a bogus rationale. The bulk scan and
# everything it needed (coverage windows, issue-first matching, proximity
# ranking, tie-breaks, contested rows, uncaptured-placeholder completion,
# unexplained-issuance reporting) was removed for that reason. If a change starts
# needing any of it back, the requirement has drifted — check it against the
# three ledgers above before writing the code.
# ---------------------------------------------------------------------------

# Rationales whose PENDING rows are worth spending a chain lookup on, i.e. the
# ones where the answer changes a token outcome.
#
# Unit Conversion is deliberately excluded. It has no balance to debit
# (_complete_balance_issuance_effect is a no-op for it) and nothing ever retries
# it — the source transfer op is not reprocessed — so every possible verdict is
# an economic no-op, while a wrong SUCCESS would permanently deny a member the
# units they converted. A stuck Unit Conversion row is an operator matter, and
# warn_stuck_pending announces it.
RESOLVABLE_RATIONALES = (PIK_RATIONALE, ABC_RATIONALE, MANAGEMENT_RATIONALE)


def _complete_balance_issuance_effect(conn, pending_row):
    """Debit the tracker a confirmed issuance was drawn from.

    This is the whole point of resolution for pik / Pending Balance Conversion:
    the tokens exist on chain, so the balance they came from must stop being
    reissued. Management has no balance column — its ledger is the log row that
    mark_issuance_success just wrote.
    """
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


def resolve_pending_issuances(
    db2,
    issuer_account=DEFAULT_ISSUER_ACCOUNT,
    token_symbol=None,
    time_budget=RESOLUTION_TIME_BUDGET,
):
    """Settle PENDING rows by asking Hive Engine about each one directly.

    Rows only reach here when the broadcast left no proof of its own fate:
    fail_pending_with_proven_non_inclusion has already failed everything whose
    recorded error shows the operation was rejected before inclusion, so what is
    left is a timeout, a transport failure, or a process that died between the
    write-ahead insert and either outcome write. Only the chain settles those.

    The lookup is scoped to one recipient, one symbol and that row's own
    35-minute window, so its cost does not grow with how long a row has been
    stuck. That is what makes an old row resolvable at all, and why no history
    scan is needed.

    Lookups run outside any open transaction — each may block for the httpx
    timeout, and holding a write transaction on token_issuance_log for that long
    stalls every other reader of the table.

    Two limits bound one pass: MAX_RESOLUTION_LOOKUPS rows and `time_budget` of
    wall clock. The row cap alone does not bound duration, because a history node
    that hangs rather than errors turns the cap into cap x timeout. Rows not
    reached are taken next cycle. Selection rotates by last_resolution_attempt
    (never-attempted first) so unresolvable rows cannot starve the rows behind
    them.

    Uncertainty leaves the row PENDING: an untrusted lookup, an unelapsed match
    window, or a legacy uncaptured sibling with a claim on the same issue.
    """
    # Absence only means anything once the match window has fully elapsed.
    settled_before = utcnow() - MATCH_CLOCK_SKEW - CHAIN_MATCH_WINDOW
    rationale_slots = ", ".join(["%s"] * len(RESOLVABLE_RATIONALES))

    with db2.engine.begin() as conn:
        rows = conn.exec_driver_sql(
            f"""
            SELECT id, recipient, units, rationale, issued_at
            FROM token_issuance_log
            WHERE status = 'PENDING'
              AND issued_at < %s
              AND rationale IN ({rationale_slots})
            ORDER BY last_resolution_attempt ASC, issued_at ASC
            LIMIT %s
            """,
            (settled_before, *RESOLVABLE_RATIONALES, MAX_RESOLUTION_LOOKUPS),
        ).fetchall()

    deadline = time.monotonic() + time_budget.total_seconds()
    attempted = 0

    for row in rows:
        if time.monotonic() >= deadline:
            print(
                f"Issuance resolution hit its time budget ({time_budget}) after "
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

            # Legacy guard. A SUCCESS row carrying UNCAPTURED_TRX_PLACEHOLDER was
            # already debited but records no trx_id, so it holds an unidentifiable
            # claim on one of these issues; taking that issue here would debit the
            # same balance twice. nectar attaches a trx_id to every broadcast it
            # signs, so this cannot arise on new rows — it exists for rows written
            # before that was true. The bound is the sibling's own reach, not
            # ours: it claims anything in [its issued_at - skew, its issued_at +
            # window], which overlaps ours whenever its issued_at is within
            # CHAIN_MATCH_WINDOW + MATCH_CLOCK_SKEW of ours.
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
            # legitimately received the same amount twice resolves one row per
            # issue. Each row commits before the next is read, so two rows sharing
            # a window in the same pass cannot both take the same issue.
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

            # More than one candidate is settled, never deferred. Nothing else ever
            # moves these rows, so deferring would leave two rows for the same
            # recipient and amount PENDING forever — the "member silently stops
            # being paid" symptom this path exists to cure. Every candidate carries
            # the same units by construction, so the choice cannot cost tokens:
            # whichever row loses its issue finds no candidate next pass, fails,
            # and reissues the identical amount. Nearest-then-trx_id is a total
            # order, so the pick is deterministic and reproducible for an auditor.
            chosen = min(
                candidates,
                key=lambda issue: (abs(issue["timestamp"] - issued_at), issue["trx_id"]),
            )
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

    if len(rows) == MAX_RESOLUTION_LOOKUPS and attempted == len(rows):
        print(
            f"Issuance resolution hit its per-pass cap ({MAX_RESOLUTION_LOOKUPS}); "
            "remaining rows are taken next cycle."
        )


def warn_stuck_pending(conn):
    """Announce PENDING rows that resolution should already have settled.

    issue_balance_tokens skips any member holding a live PENDING row for the same
    rationale, so an unresolvable row silently stops that member's dividends.

    Causes worth operator attention: an unreachable history API (every lookup
    returning unknown), a legacy uncaptured sibling holding a claim on the same
    window, a backlog deeper than one pass's budget, or a Unit Conversion row,
    which is never resolved automatically and can only be settled by hand.
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
    """Settle last cycle's unconfirmed intents before issuing anything new.

    Cheapest first: a row whose recorded broadcast error already proves the
    operation never reached the chain needs no chain evidence at all. That is one
    read and a keyed update, and it clears the whole class of row that caused the
    stall this work was opened for.
    """
    with db2.engine.begin() as conn:
        swept = fail_pending_with_proven_non_inclusion(conn)
    if swept:
        print(
            f"Failed {swept} PENDING issuance(s) whose recorded error proves the "
            "broadcast never reached the chain; they re-issue next cycle."
        )

    # The symbol and account travel with the issuer. Defaulting either would check
    # a non-HSBIDAO issuer's rows against HSBIDAO history, find nothing, and fail
    # every one of them.
    resolve_pending_issuances(
        db2,
        issuer_account=issuer.account_name,
        token_symbol=issuer.token_symbol,
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
