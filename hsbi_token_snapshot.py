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
    insert_pending_issuance,
    issue_trx_id,
    log_issuance,
    mark_issuance_failure,
    mark_issuance_success,
    record_pending_error,
    utcnow,
)
from hivesbi.issue import (
    get_tokenholders,
    get_default_token_issuer,
)


BATCH_SLEEP_TIME = 3
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
CHAIN_SCAN_LOOKBACK = timedelta(hours=5)
HISTORY_SCAN_LIMIT = 1000
HISTORY_SCAN_HARD_LIMIT = 10000
MATCH_CLOCK_SKEW = timedelta(minutes=5)


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
    saw_timestamp = False
    for history_row in history:
        scanned += 1
        if scanned > limit:
            break
        row, op = _unwrap_history_op(history_row)
        timestamp = ensure_timezone_aware(row.get("timestamp") or op.get("timestamp"))
        if timestamp is not None:
            saw_timestamp = True
        if timestamp is not None and timestamp < cutoff:
            reached_cutoff = True
            break
        issue = _parse_engine_issue(history_row, token_symbol=token_symbol)
        if issue is not None:
            issuances.append(issue)

    complete = reached_cutoff or scanned < limit
    if reached_cutoff:
        covered_since = cutoff
    elif complete and (saw_timestamp or scanned == 0):
        covered_since = datetime.min.replace(tzinfo=timezone.utc)
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


def _match_pending_rows_to_chain(pending_rows, chain_issuances):
    """Match on-chain issues to the closest compatible PENDING row.

    Hive Engine issuance does not carry our rationale, and the current
    nectarengine issue helper cannot add a memo. Timestamp proximity is the best
    durable discriminator when a recipient receives the same amount for multiple
    rationales.
    """
    matches = {}
    used_log_ids = set()
    used_trx_ids = set()

    def chain_sort_key(chain_issue):
        return chain_issue.get("timestamp") or datetime.max.replace(tzinfo=timezone.utc)

    for chain_issue in sorted(chain_issuances, key=chain_sort_key):
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
        else:
            ranked = sorted(
                candidates,
                key=lambda row: abs(chain_timestamp - row["issued_at"]),
            )
            if len(ranked) > 1 and abs(
                chain_timestamp - ranked[0]["issued_at"]
            ) == abs(chain_timestamp - ranked[1]["issued_at"]):
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
    scan_complete=False,
):
    """Resolve write-ahead PENDING rows against recent on-chain issuances.

    Reconciliation uses the blockchain timestamp as the authority. A PENDING row
    is confirmed only by a matching chain issue (recipient + units + chain
    timestamp near the intent timestamp). A PENDING row is failed only when the
    chain scan provably covered its entire possible match window AND no in-window
    chain issue could plausibly be its broadcast (a contested row stays PENDING;
    once the sibling SUCCESS row records the chain trx_id, the next pass excludes
    that issue and the row resolves).

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
            scan_complete
            and covered_since is not None
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

    unmatched_chain = [
        c for c in unmatched_chain if c["trx_id"] not in matched_trx_ids
    ]
    for chain_issue in unmatched_chain:
        uncaptured = _find_logged_success_for_chain_issue(
            conn, chain_issue, trx_id=UNCAPTURED_TRX_PLACEHOLDER
        )
        if uncaptured is not None:
            complete_uncaptured_success(conn, uncaptured["id"], chain_issue["trx_id"])
            print(
                "Completed logged issuance from chain: "
                f"{chain_issue['trx_id']} {chain_issue['recipient']} "
                f"{chain_issue['units']} {uncaptured['rationale']}"
            )
            continue

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


def reconcile_recent_issuances(db2, issuer):
    lookback = CHAIN_SCAN_LOOKBACK
    with db2.engine.begin() as conn:
        oldest_pending = conn.exec_driver_sql(
            "SELECT MIN(issued_at) FROM token_issuance_log WHERE status = 'PENDING'"
        ).scalar()
    oldest_pending = ensure_timezone_aware(oldest_pending)
    if oldest_pending is not None:
        needed = utcnow() - oldest_pending + MATCH_CLOCK_SKEW + CHAIN_MATCH_WINDOW
        if needed > lookback:
            lookback = needed

    # Scale the scan budget with the lookback: a fixed limit on a busy issuer
    # (one custom_json per member issuance) would stop the scan before the
    # cutoff, complete=False forever, and old PENDING rows could never resolve.
    limit = HISTORY_SCAN_LIMIT
    if lookback > CHAIN_SCAN_LOOKBACK:
        limit = min(
            int(HISTORY_SCAN_LIMIT * (lookback / CHAIN_SCAN_LOOKBACK)) + 1,
            HISTORY_SCAN_HARD_LIMIT,
        )

    scan = fetch_recent_chain_issuance_scan(issuer, lookback=lookback, limit=limit)
    with db2.engine.begin() as conn:
        reconcile_issuances(
            conn,
            scan["issuances"],
            covered_since=scan["covered_since"],
            covered_until=scan["covered_until"],
            scan_complete=scan["complete"],
        )


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

    for i, row in enumerate(balance_rows):
        member_name = row[0]
        amount = floor_token_amount(row[1])
        if amount < TOKEN_PRECISION:
            continue
        if i > 0 and i % 5 == 0:
            print(f"Sleeping for {BATCH_SLEEP_TIME} seconds...")
            time.sleep(BATCH_SLEEP_TIME)

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
            print(f"Failed to issue to {member_name}: {e}")
            with db2.engine.begin() as conn:
                record_pending_error(conn, log_id, str(e))
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
        print(f"Failed Management issuance to {MANAGEMENT_RECIPIENT}: {e}")
        with db2.engine.begin() as conn:
            record_pending_error(conn, log_id, str(e))
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
        time.sleep(BATCH_SLEEP_TIME)

        # Pending Balance Conversion issuance
        issue_balance_tokens(db2, issuer, ABC_RATIONALE, "abc_pik")
        time.sleep(BATCH_SLEEP_TIME)

        # Refresh liquid balances from Hive Engine, then issue the Management 10%
        # against real circulating supply only.
        sync_tokenholders(db2)
        issue_management_tokens(db2, issuer)


if __name__ == "__main__":
    main()
