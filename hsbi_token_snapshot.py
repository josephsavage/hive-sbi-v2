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

# rationale -> tokenholders balance column that gets debited once an issuance of
# that rationale is confirmed. Management issuance debits no balance.
RATIONALE_BALANCE_COLUMN = {
    PIK_RATIONALE: "pik",
    ABC_RATIONALE: "abc_pik",
}

RECONCILIATION_WINDOW = timedelta(hours=5)
HISTORY_SCAN_LIMIT = 1000
PENDING_TRX_PLACEHOLDER = "PENDING"


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
# write-ahead issuance log helpers
# ---------------------------------------------------------------------------
# Every broadcast is preceded by a committed PENDING row carrying its true
# rationale. The PENDING row makes the issuance durable across a crash WITHOUT
# consulting the chain for rationale: the Management cap counts PENDING+SUCCESS so
# an unconfirmed issuance cannot be re-issued, and per-member balances are only
# debited once the issuance is confirmed (here on success, or later by chain
# reconciliation). status moves PENDING -> SUCCESS/FAILURE; rationale never changes.


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


def debit_balance(conn, rationale, recipient, units):
    """Subtract the exact issued amount from the member's source balance.

    Subtracting (rather than zeroing) preserves any accrual added between selection
    and confirmation. Management issuance maps to no column and debits nothing.
    """
    column = RATIONALE_BALANCE_COLUMN.get(rationale)
    if column is None:
        return
    conn.exec_driver_sql(
        f"UPDATE tokenholders SET {column} = {column} - %s WHERE member_name = %s",
        (units, recipient),
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
    }


def fetch_recent_chain_issuances(issuer, token_symbol="HSBIDAO", limit=HISTORY_SCAN_LIMIT):
    cutoff = datetime.now(timezone.utc) - RECONCILIATION_WINDOW
    try:
        history = issuer.hive_account.history_reverse(only_ops=["custom_json"])
    except TypeError:
        try:
            history = issuer.hive_account.history_reverse()
        except Exception as exc:
            print(f"Unable to fetch issuer history for reconciliation: {exc}")
            return []
    except Exception as exc:
        print(f"Unable to fetch issuer history for reconciliation: {exc}")
        return []

    issuances = []
    scanned = 0
    for history_row in history:
        scanned += 1
        if scanned > limit:
            break
        row, op = _unwrap_history_op(history_row)
        timestamp = ensure_timezone_aware(row.get("timestamp") or op.get("timestamp"))
        if timestamp is not None and timestamp < cutoff:
            break
        issue = _parse_engine_issue(history_row, token_symbol=token_symbol)
        if issue is not None:
            issuances.append(issue)
    return issuances


def reconcile_issuances(conn, chain_issuances, now=None):
    """Resolve PENDING issuance rows against recent on-chain issuances.

    - A PENDING row that matches an on-chain issuance (same recipient + units) is
      confirmed SUCCESS and its source balance is debited once.
    - A PENDING row older than the reconciliation window with no on-chain match is
      marked FAILURE so it stops counting toward the Management cap and the member
      becomes eligible for a fresh attempt.
    - An on-chain issuance with no matching log row is an out-of-band issuance; it
      is recorded for audit with rationale='reconciled' and never participates in
      cap math or balance debits.

    rationale is read from the existing PENDING row and is never inferred from the
    chain or rewritten.
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
    unmatched_chain = [c for c in chain_issuances if c["trx_id"] not in logged_trx]

    pending_rows = conn.exec_driver_sql(
        """
        SELECT id, recipient, units, rationale, issued_at
        FROM token_issuance_log
        WHERE status = 'PENDING'
        ORDER BY issued_at ASC
        """
    ).fetchall()

    for row in pending_rows:
        log_id, recipient, units, rationale, issued_at = (
            row[0],
            row[1],
            token_decimal(row[2]),
            row[3],
            row[4],
        )
        match = next(
            (
                c
                for c in unmatched_chain
                if c["recipient"] == recipient and token_decimal(c["units"]) == units
            ),
            None,
        )
        if match is not None:
            unmatched_chain.remove(match)
            mark_issuance_success(conn, log_id, match["trx_id"])
            debit_balance(conn, rationale, recipient, units)
            print(
                f"Reconciled PENDING -> SUCCESS: {recipient} {units} "
                f"{rationale} ({match['trx_id']})"
            )
            continue

        issued_at_aware = ensure_timezone_aware(issued_at)
        if issued_at_aware is not None and issued_at_aware < stale_before:
            mark_issuance_failure(
                conn,
                log_id,
                "No matching on-chain issuance within reconciliation window",
            )
            print(f"Reconciled PENDING -> FAILURE: {recipient} {units} {rationale}")

    for chain_issue in unmatched_chain:
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
    chain_issuances = fetch_recent_chain_issuances(issuer)
    with db2.engine.begin() as conn:
        reconcile_issuances(conn, chain_issuances)


# ---------------------------------------------------------------------------
# issuance paths
# ---------------------------------------------------------------------------


def select_issuable_balances(conn, balance_column, rationale):
    """Members with a positive balance and no in-flight PENDING row for this
    rationale (so an unconfirmed issuance is never duplicated)."""
    return conn.exec_driver_sql(
        f"""
        SELECT t.member_name, t.{balance_column} AS amount
        FROM tokenholders t
        WHERE t.{balance_column} > 0
          AND NOT EXISTS (
              SELECT 1 FROM token_issuance_log l
              WHERE l.recipient = t.member_name
                AND l.rationale = %s
                AND l.status = 'PENDING'
          )
        """,
        (rationale,),
    ).fetchall()


def issue_balance_tokens(db2, issuer, rationale, balance_column):
    """Write-ahead issuance for per-member balances (pik / abc_pik).

    Members with an in-flight PENDING row for this rationale are skipped so an
    unconfirmed issuance is never duplicated; chain reconciliation resolves those
    rows before the member becomes eligible again.
    """
    with db2.engine.begin() as conn:
        pending_rows = select_issuable_balances(conn, balance_column, rationale)

    for i, row in enumerate(pending_rows):
        member_name = row[0]
        amount = floor_token_amount(row[1])
        if amount < TOKEN_PRECISION:
            continue
        if i > 0 and i % 5 == 0:
            print(f"Sleeping for {BATCH_SLEEP_TIME} seconds...")
            time.sleep(BATCH_SLEEP_TIME)

        # 1. write-ahead: commit the PENDING row before broadcasting
        with db2.engine.begin() as conn:
            log_id = insert_pending_issuance(conn, member_name, amount, rationale)

        # 2. broadcast (the only step that cannot be rolled back)
        print(f"Issuing {amount} HSBIDAO ({rationale}) to {member_name}")
        try:
            tx = issuer.issue(member_name, float(amount))
            trx_id = tx.get("trx_id") or tx.get("transaction_id") or "N/A"
            print("Issued:", tx)
        except Exception as e:
            print(f"Failed to issue to {member_name}: {e}")
            with db2.engine.begin() as conn:
                record_pending_error(conn, log_id, str(e))
            continue

        # 3. confirm success and debit the issued amount in one transaction
        with db2.engine.begin() as conn:
            mark_issuance_success(conn, log_id, trx_id)
            debit_balance(conn, rationale, member_name, amount)


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
