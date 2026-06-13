import json
import time
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from hivesbi.settings import get_runtime
from hivesbi.storage import ConfigurationDB
from hivesbi.utils import ensure_timezone_aware
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
RECONCILED_RATIONALE = "reconciled"

RECONCILIATION_WINDOW = timedelta(hours=5)
HISTORY_SCAN_LIMIT = 1000
PENDING_TRX_PLACEHOLDER = "PENDING"
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
# write-ahead issuance log helpers (Management issuance only)
# ---------------------------------------------------------------------------
# Only the Management 10% issuance uses the write-ahead protocol, because it is
# capped (a double issuance permanently over-mints) and has no per-member balance
# to retry against. The Management broadcast is preceded by a committed PENDING row
# carrying rationale='Management'; the cap counts PENDING+SUCCESS so a crash after
# broadcast cannot re-issue, and chain reconciliation later resolves the row to
# SUCCESS or FAILURE. status moves PENDING -> SUCCESS/FAILURE; rationale never
# changes.
#
# Per-member pik / abc_pik issuance deliberately does NOT use this protocol — it is
# immediate and self-healing (see issue_balance_tokens): a failed broadcast simply
# leaves the balance to be retried next cycle, so a transient failure can never
# permanently strand a member's dividends behind an unresolved PENDING row.


def insert_pending_issuance(conn, recipient, units, rationale):
    result = conn.exec_driver_sql(
        """
        INSERT INTO token_issuance_log
            (trx_id, recipient, units, status, error_message, rationale)
        VALUES (%s, %s, %s, 'PENDING', NULL, %s)
        """,
        (PENDING_TRX_PLACEHOLDER, recipient, units, rationale),
    )
    return result.lastrowid


def mark_issuance_success(conn, log_id, trx_id):
    conn.exec_driver_sql(
        "UPDATE token_issuance_log SET status = 'SUCCESS', trx_id = %s WHERE id = %s",
        (trx_id, log_id),
    )


def mark_issuance_failure(conn, log_id, error_message):
    conn.exec_driver_sql(
        "UPDATE token_issuance_log SET status = 'FAILURE', error_message = %s WHERE id = %s",
        (error_message, log_id),
    )


def record_pending_error(conn, log_id, error_message):
    """Keep a row PENDING (so it still guards against re-issue) but note the error.

    A broadcast that raised may still have reached the chain, so we do not assume
    failure here — chain reconciliation resolves the row to SUCCESS or FAILURE.
    """
    conn.exec_driver_sql(
        "UPDATE token_issuance_log SET error_message = %s WHERE id = %s",
        (error_message, log_id),
    )


def log_issuance(conn, trx_id, recipient, units, status, rationale, error_message=None):
    """Append a terminal (SUCCESS/FAILURE) issuance row.

    Used by the immediate pik / abc_pik path, which logs the outcome directly
    rather than going through the Management write-ahead PENDING protocol.
    """
    conn.exec_driver_sql(
        """
        INSERT INTO token_issuance_log
            (trx_id, recipient, units, status, error_message, rationale)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (trx_id, recipient, units, status, error_message, rationale),
    )


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
    issuer, token_symbol="HSBIDAO", limit=HISTORY_SCAN_LIMIT
):
    cutoff = datetime.now(timezone.utc) - RECONCILIATION_WINDOW
    try:
        history = issuer.hive_account.history_reverse(only_ops=["custom_json"])
    except TypeError:
        try:
            history = issuer.hive_account.history_reverse()
        except Exception as exc:
            print(f"Unable to fetch issuer history for reconciliation: {exc}")
            return {"issuances": [], "covered_since": None, "complete": False}
    except Exception as exc:
        print(f"Unable to fetch issuer history for reconciliation: {exc}")
        return {"issuances": [], "covered_since": None, "complete": False}

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
    elif complete and saw_timestamp:
        covered_since = datetime.min.replace(tzinfo=timezone.utc)
    else:
        covered_since = None
    return {"issuances": issuances, "covered_since": covered_since, "complete": complete}


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
        return True
    return issued_at - MATCH_CLOCK_SKEW <= chain_timestamp <= issued_at + RECONCILIATION_WINDOW


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
            timestamped_candidates = [row for row in candidates if row.get("issued_at")]
            if len(timestamped_candidates) != 1:
                continue
            best = timestamped_candidates[0]
        else:
            best = min(
                candidates,
                key=lambda row: abs(chain_timestamp - row["issued_at"])
                if row.get("issued_at") is not None
                else RECONCILIATION_WINDOW,
            )
        matches[best["id"]] = chain_issue
        used_log_ids.add(best["id"])
        used_trx_ids.add(chain_issue["trx_id"])
    return matches, used_trx_ids


def reconcile_issuances(
    conn,
    chain_issuances,
    now=None,
    covered_since=None,
    scan_complete=False,
):
    """Resolve Management write-ahead PENDING rows against recent on-chain issuances.

    Only the Management 10% issuance uses the write-ahead PENDING protocol, so this
    routine is rationale-scoped to Management. Per-member pik / abc_pik issuance is
    immediate and self-healing (see issue_balance_tokens) and never produces PENDING
    rows, so it is never touched here.

    - A Management PENDING row that matches an on-chain issuance (same recipient +
      units, nearest timestamp) is confirmed SUCCESS. Management debits no balance.
    - A Management PENDING row older than the reconciliation window with no on-chain
      match — only when the scan provably covered its issued_at — is marked FAILURE
      so it stops holding down the Management cap and can be re-attempted.
    - An on-chain issuance with no log row, and not already explained by a confirmed
      issuance (matched trx_id, or a SUCCESS row of the same recipient + units within
      the window), is an out-of-band issuance recorded for audit with
      rationale='reconciled'. It never participates in cap math.

    rationale is read from the existing PENDING row and is never inferred from the
    chain or rewritten. Confirmed pik / abc dividends are recognised by their logged
    trx_id (and, defensively, recipient + units), so a same-amount member dividend to
    josephsavage is never mistaken for a Management issuance.
    """
    now = now or datetime.now(timezone.utc)
    stale_before = now - RECONCILIATION_WINDOW

    logged_trx = {
        r[0]
        for r in conn.exec_driver_sql(
            "SELECT trx_id FROM token_issuance_log WHERE trx_id <> %s",
            (PENDING_TRX_PLACEHOLDER,),
        ).fetchall()
    }
    # Confirmed issuances within the window, keyed by (recipient, units). Used to
    # suppress duplicate orphan audit rows for a pik / abc issuance whose on-chain
    # trx_id was not captured in its log row.
    explained = {
        (r[0], token_decimal(r[1]))
        for r in conn.exec_driver_sql(
            """
            SELECT recipient, units FROM token_issuance_log
            WHERE status = 'SUCCESS' AND trx_id <> %s AND issued_at >= %s
            """,
            (PENDING_TRX_PLACEHOLDER, stale_before),
        ).fetchall()
    }
    unmatched_chain = [c for c in chain_issuances if c["trx_id"] not in logged_trx]

    pending_rows = conn.exec_driver_sql(
        """
        SELECT id, recipient, units, rationale, issued_at
        FROM token_issuance_log
        WHERE status = 'PENDING' AND rationale = %s
        ORDER BY issued_at ASC
        """,
        (MANAGEMENT_RATIONALE,),
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

    for row in pending_rows:
        log_id, recipient, units, rationale, issued_at = (
            row[0],
            row[1],
            token_decimal(row[2]),
            row[3],
            row[4],
        )
        match = matches.get(log_id)
        if match is not None:
            mark_issuance_success(conn, log_id, match["trx_id"])
            print(
                f"Reconciled PENDING -> SUCCESS: {recipient} {units} "
                f"{rationale} ({match['trx_id']})"
            )
            continue

        issued_at_aware = ensure_timezone_aware(issued_at)
        scan_covers_row = (
            scan_complete
            and covered_since is not None
            and issued_at_aware is not None
            and covered_since <= issued_at_aware
        )
        if issued_at_aware is not None and issued_at_aware < stale_before and scan_covers_row:
            mark_issuance_failure(
                conn,
                log_id,
                "No matching on-chain issuance within reconciliation window",
            )
            print(f"Reconciled PENDING -> FAILURE: {recipient} {units} {rationale}")

    unmatched_chain = [
        c for c in unmatched_chain if c["trx_id"] not in matched_trx_ids
    ]
    for chain_issue in unmatched_chain:
        if (chain_issue["recipient"], token_decimal(chain_issue["units"])) in explained:
            # Already explained by a confirmed pik / abc issuance whose trx_id was
            # not captured in its log row — do not double-log it as out-of-band.
            continue
        conn.exec_driver_sql(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale)
            VALUES (%s, %s, %s, 'SUCCESS', %s, %s)
            """,
            (
                chain_issue["trx_id"],
                chain_issue["recipient"],
                chain_issue["units"],
                "out-of-band issuance recorded for audit",
                RECONCILED_RATIONALE,
            ),
        )
        print(
            "Audit-logged unexplained on-chain issuance: "
            f"{chain_issue['trx_id']} {chain_issue['recipient']} {chain_issue['units']}"
        )


def reconcile_recent_issuances(db2, issuer):
    scan = fetch_recent_chain_issuance_scan(issuer)
    with db2.engine.begin() as conn:
        reconcile_issuances(
            conn,
            scan["issuances"],
            covered_since=scan["covered_since"],
            scan_complete=scan["complete"],
        )


# ---------------------------------------------------------------------------
# issuance paths
# ---------------------------------------------------------------------------


def issue_balance_tokens(db2, issuer, rationale, balance_column):
    """Immediate, self-healing issuance for per-member balances (pik / abc_pik).

    Each member with a positive balance is issued tokens; only on a confirmed
    broadcast is the balance zeroed and a SUCCESS row logged. A failed broadcast
    logs FAILURE and leaves the balance intact, so the member is simply retried on
    the next cycle — a transient failure is temporary, never permanent.

    This deliberately uses NO write-ahead PENDING guard: the guard exists for the
    capped Management issuance (where a double issuance over-mints), but for a
    per-member balance the safe failure mode is "retry next cycle", not "block the
    member until reconciliation". See the write-ahead helper note above and
    CHANGES.md.
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

        # 1. broadcast (the only step that cannot be rolled back)
        print(f"Issuing {amount} HSBIDAO ({rationale}) to {member_name}")
        try:
            tx = issuer.issue(member_name, float(amount))
            trx_id = tx.get("trx_id") or tx.get("transaction_id") or "N/A"
            print("Issued:", tx)
        except Exception as e:
            print(f"Failed to issue to {member_name}: {e}")
            # Leave the balance intact for retry next cycle; just record the failure.
            with db2.engine.begin() as conn:
                log_issuance(
                    conn,
                    trx_id="N/A",
                    recipient=member_name,
                    units=amount,
                    status="FAILURE",
                    rationale=rationale,
                    error_message=str(e),
                )
            continue

        # 2. zero the issued balance and log SUCCESS in one transaction
        with db2.engine.begin() as conn:
            conn.exec_driver_sql(
                f"UPDATE tokenholders SET {balance_column} = 0 WHERE member_name = %s",
                (member_name,),
            )
            log_issuance(
                conn,
                trx_id=trx_id,
                recipient=member_name,
                units=amount,
                status="SUCCESS",
                rationale=rationale,
            )


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
        trx_id = tx.get("trx_id") or tx.get("transaction_id") or "N/A"
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
