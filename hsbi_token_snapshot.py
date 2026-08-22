from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from hivesbi.settings import get_runtime
from hivesbi.storage import ConfigurationDB
from hivesbi.utils import ensure_timezone_aware
from hivesbi.issuance_log import (
    fail_pending_with_proven_non_inclusion,
    insert_pending_issuance,
    issue_trx_id,
    log_issuance,
    mark_issuance_success,
    record_broadcast_error,
    utcnow,
)
from hivesbi.issue import (
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
# ---------------------------------------------------------------------------
# unconfirmed issuances
#
# THE REMIT - read this before adding anything here.
#
# Four paths issue HSBIDAO and each retries safely on its own. The write-ahead
# log exists for the one thing they cannot survive: an issuance that reached the
# chain while the record that would stop it being repeated did not get written.
#
#   pik / Pending Balance Conversion - the ledger is tokenholders.pik / .abc_pik.
#       The balance is debited only in the same transaction that records SUCCESS,
#       so a broadcast that fails leaves the balance intact and next cycle simply
#       reissues it. The one unrecoverable outcome is issued-but-not-debited: the
#       same tokens are minted again next cycle, and a mint cannot be undone.
#   Management - the ledger is this log: the 10% cap counts SUM(units) over
#       SUCCESS + PENDING rows. Issued-but-not-logged undercounts the cap and
#       management is overpaid next cycle.
#   Unit Conversion - no balance to debit, and nothing retries it because the
#       source transfer op is never reprocessed.
#
# So a PENDING row is HELD, not settled. It blocks its (recipient, rationale)
# from being issued again, which is the safe direction: an unpaid member is
# recoverable, a double mint is not. Exactly two things move a row off PENDING,
# and both decide from the row's own recorded error rather than from the chain:
#
#   record_broadcast_error fails it at broadcast time when the error proves the
#       operation never reached the chain, so the next cycle reissues it.
#   fail_pending_with_proven_non_inclusion applies that same test to rows already
#       stuck. One read and a keyed update, no network.
#
# What survives both is a row whose error does not prove its own fate - a
# timeout, a transport failure, a process that died mid-flight. Nothing settles
# those automatically: warn_stuck_pending announces them and an operator decides.
#
# That last part is a deliberate limit, not an omission. Settling from chain
# history means asking a node picked at runtime whether an issuance exists, then
# minting or debiting a balance on its answer - and a wrong answer is
# unrecoverable in both directions. PR #138 did walk the issuer's custom_json
# history to match the log in bulk and duplicated every Unit Conversion row under
# a bogus rationale. PR #140 replaced that with a per-row Hive Engine lookup,
# which was narrower but bought nothing: every PENDING row prod has accumulated
# carries a HIVE_CUSTOM_OP_BLOCK_LIMIT error, which the two checks above settle
# outright, so the lookup path never had a row to decide. It was removed along
# with its httpx history client rather than carried as untested weight.
#
# If a change starts needing chain history back, that is the requirement drifting
# - check it against the three ledgers above before writing the code.
# ---------------------------------------------------------------------------
# The tokenholders column each rationale draws from, and the only place that
# pairing is written down. It is also an allow-list: a column name cannot be a
# bound parameter, so every statement below interpolates it into SQL directly.
# Deriving it here rather than taking it from the caller keeps anything a caller
# controls out of that interpolation for good.
BALANCE_COLUMNS = {
    PIK_RATIONALE: "pik",
    ABC_RATIONALE: "abc_pik",
}


def warn_stuck_pending(conn):
    """Announce PENDING rows old enough that nothing is going to clear them.

    issue_balance_tokens skips any member holding a live PENDING row for the same
    rationale, so a row left here silently stops that member's dividends. This is
    the only thing watching for that, because a row that reaches this age has
    already survived the one automatic check there is (see THE REMIT above) and
    can only be settled by hand.

    Deciding one takes the same evidence an operator would gather anyway: whether
    the recipient holds an HSBIDAO issuance from us near issued_at. If they do,
    the tokens exist - mark the row SUCCESS and debit the matching tokenholders
    balance in one transaction. If they do not, mark it FAILURE and the next cycle
    reissues it. Do not guess: the row is blocking one member's dividends, which
    is recoverable, and both wrong answers mint or destroy tokens, which is not.
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


def reconcile_recent_issuances(db2):
    """Settle last cycle's unconfirmed intents before issuing anything new.

    A row whose recorded broadcast error already proves the operation never
    reached the chain needs no chain evidence at all: one read and a keyed
    update, and it clears the whole class of row that caused the stall this work
    was opened for. Anything that survives that test has no proof of its own fate
    and is left PENDING for an operator - see THE REMIT above.
    """
    with db2.engine.begin() as conn:
        swept = fail_pending_with_proven_non_inclusion(conn)
    if swept:
        print(
            f"Failed {swept} PENDING issuance(s) whose recorded error proves the "
            "broadcast never reached the chain; they re-issue next cycle."
        )

    with db2.engine.begin() as conn:
        warn_stuck_pending(conn)


# ---------------------------------------------------------------------------
# issuance paths
# ---------------------------------------------------------------------------


def issue_balance_tokens(db2, issuer, rationale):
    """Write-ahead issuance for per-member balances (pik / abc_pik).

    Each member with a positive balance gets a committed PENDING intent before
    broadcast. If the process dies after broadcast and before SUCCESS is recorded,
    the row stays PENDING and keeps blocking this member's next issuance for this
    rationale. Nothing completes it from chain history any more: it is settled
    only by its own recorded error, or by an operator (see THE REMIT above).

    The balance column comes from BALANCE_COLUMNS, not from the caller: it is
    interpolated into SQL below and a column name cannot be bound as a parameter.
    """
    balance_column = BALANCE_COLUMNS[rationale]
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
        reconcile_recent_issuances(db2)

        # curation PIK token issuance
        issue_balance_tokens(db2, issuer, PIK_RATIONALE)

        # Pending Balance Conversion issuance
        issue_balance_tokens(db2, issuer, ABC_RATIONALE)

        # Refresh liquid balances from Hive Engine, then issue the Management 10%
        # against real circulating supply only.
        sync_tokenholders(db2)
        issue_management_tokens(db2, issuer)


if __name__ == "__main__":
    main()
