import json
import time
from decimal import Decimal, ROUND_DOWN
from sqlalchemy import text
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
MANAGEMENT_RATIONALE = "Management"
RECONCILIATION_WINDOW = timedelta(hours=5)


def token_decimal(value):
    return Decimal(str(value or "0"))


def floor_token_amount(value):
    return token_decimal(value).quantize(TOKEN_PRECISION, rounding=ROUND_DOWN)


def calculate_management_issue_amount(outstanding, management_success):
    return floor_token_amount(
        ((Decimal("0.10") * token_decimal(outstanding)) - token_decimal(management_success))
        / Decimal("0.90")
    )


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


def fetch_recent_chain_issuances(issuer, token_symbol="HSBIDAO"):
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
    for history_row in history:
        row, op = _unwrap_history_op(history_row)
        timestamp = ensure_timezone_aware(row.get("timestamp") or op.get("timestamp"))
        if timestamp is not None and timestamp < cutoff:
            break
        issue = _parse_engine_issue(history_row, token_symbol=token_symbol)
        if issue is not None:
            issuances.append(issue)
    return issuances


def reconcile_recent_issuances(conn, issuer):
    for issue in fetch_recent_chain_issuances(issuer):
        existing = conn.exec_driver_sql(
            "SELECT id FROM token_issuance_log WHERE trx_id = %s LIMIT 1",
            (issue["trx_id"],),
        ).fetchone()
        if existing is not None:
            continue
        rationale = "reconciled"
        conn.exec_driver_sql(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale)
            VALUES (%s, %s, %s, 'SUCCESS', NULL, %s)
            """,
            (issue["trx_id"], issue["recipient"], issue["units"], rationale),
        )
        print(
            "Backfilled token issuance log: "
            f"{issue['trx_id']} {issue['recipient']} {issue['units']} {rationale}"
        )


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


def issue_management_tokens(db2, issuer):
    with db2.engine.begin() as conn:
        management_success = token_decimal(
            conn.exec_driver_sql(
                """
                SELECT COALESCE(SUM(units), 0) AS units
                FROM token_issuance_log
                WHERE status = 'SUCCESS' AND rationale = %s
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

    issue_amount = calculate_management_issue_amount(outstanding, management_success)
    if issue_amount < TOKEN_PRECISION:
        print(
            "Skipping Management issuance: "
            f"outstanding={outstanding} management_success={management_success}"
        )
        return

    print(f"Issuing {issue_amount} HSBIDAO Management tokens to {MANAGEMENT_RECIPIENT}")
    try:
        tx = issuer.issue(MANAGEMENT_RECIPIENT, float(issue_amount))
        trx_id = tx.get("trx_id") or tx.get("transaction_id") or "N/A"
        print("Issued:", tx)
    except Exception as e:
        print(f"Failed Management issuance to {MANAGEMENT_RECIPIENT}: {e}")
        with db2.engine.begin() as conn:
            conn.exec_driver_sql(
                """
                INSERT INTO token_issuance_log
                    (trx_id, recipient, units, status, error_message, rationale)
                VALUES (%s, %s, %s, 'FAILURE', %s, %s)
                """,
                ("N/A", MANAGEMENT_RECIPIENT, issue_amount, str(e), MANAGEMENT_RATIONALE),
            )
        return

    with db2.engine.begin() as conn:
        conn.exec_driver_sql(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale)
            VALUES (%s, %s, %s, 'SUCCESS', NULL, %s)
            """,
            (trx_id, MANAGEMENT_RECIPIENT, issue_amount, MANAGEMENT_RATIONALE),
        )


def main():
    rt = get_runtime()
    cfg = rt["cfg"]

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
        with db2.engine.begin() as conn:
            reconcile_recent_issuances(conn, issuer)

        # curation PIK token issuance
        with db2.engine.begin() as conn:
            # issue tokens for members with pik > 0
            pending_rows = conn.exec_driver_sql(
                "SELECT member_name, pik FROM tokenholders WHERE pik > 0"
            ).fetchall()

            for i, (member_name, pik) in enumerate(pending_rows):
                if i > 0 and i % 5 == 0:
                    print(f"Sleeping for {BATCH_SLEEP_TIME} seconds...")
                    time.sleep(BATCH_SLEEP_TIME)
                print(f"Issuing {pik} HSBIDAO to {member_name}")
                try:
                    # 1. Blockchain side-effect (cannot be rolled back)

                    tx = issuer.issue(member_name, float(pik))
                    trx_id = tx.get("trx_id")  # extract the string

                    print("Issued:", tx)

                    # 2. Now open a DB transaction for the state change + audit log
                    with db2.engine.begin() as conn:
                        conn.execute(
                            text("UPDATE tokenholders SET pik = 0 WHERE member_name = :m"),
                            {"m": member_name},
                        )

                        conn.execute(
                            text("""
                                INSERT INTO token_issuance_log
                                    (trx_id, recipient, units, status, error_message, rationale)
                                VALUES
                                    (:trx, :recipient, :units, 'SUCCESS', NULL, 'pik')
                            """),
                            {"trx": trx_id, "recipient": member_name, "units": pik},
                        )

                except Exception as e:
                    print(f"Failed to issue to {member_name}: {e}")

                    # Log failure in its own transaction
                    with db2.engine.begin() as conn:
                        conn.execute(
                            text("""
                                INSERT INTO token_issuance_log
                                    (trx_id, recipient, units, status, error_message, rationale)
                                VALUES
                                    ('N/A', :recipient, :units, 'FAILURE', :err, 'FAILURE')
                            """),
                            {"recipient": member_name, "units": pik, "err": str(e)},
                        )
        time.sleep(BATCH_SLEEP_TIME)

        # Pending Balance Conversion logic here
        with db2.engine.begin() as conn:
            pending_rows = conn.exec_driver_sql(
                "SELECT member_name, abc_pik FROM tokenholders WHERE abc_pik > 0"
            ).fetchall()

        for i, (member_name, abc_pik) in enumerate(pending_rows):
            if i > 0 and i % 5 == 0:
                print(f"Sleeping for {BATCH_SLEEP_TIME} seconds...")
                time.sleep(BATCH_SLEEP_TIME)
                    
            print(f"Issuing {abc_pik} HSBIDAO to {member_name}")
            try:
                tx = issuer.issue(member_name, float(abc_pik))
                trx_id = tx.get("trx_id")  # extract the string
                print("Issued:", tx)
                    
            except Exception as e:
                print(f"Failed to issue to {member_name}: {e}")
                    
                # Log failure
                with db2.engine.begin() as conn:
                    conn.exec_driver_sql(
                        """
                        INSERT INTO token_issuance_log
                            (trx_id, recipient, units, status, error_message, rationale)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        ("N/A", member_name, abc_pik, "FAILURE", str(e), "FAILURE"),
                    )
                continue

            # Reset abc_pik to 0 after successful issuance
            with db2.engine.begin() as conn:
                conn.exec_driver_sql(
                    "UPDATE tokenholders SET abc_pik = 0 WHERE member_name = %s",
                    (member_name,),
                )
                    
                # Log success
                conn.exec_driver_sql(
                    """
                    INSERT INTO token_issuance_log 
                        (trx_id, recipient, units, status, error_message, rationale)
                    VALUES (%s, %s, %s, %s, NULL, %s)
                    """,
                    (trx_id, member_name, abc_pik, "SUCCESS", "Pending Balance Conversion"),
                )

        time.sleep(BATCH_SLEEP_TIME)

        sync_tokenholders(db2)
        issue_management_tokens(db2, issuer)


if __name__ == "__main__":
    main()
