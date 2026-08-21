"""Write-ahead issuance log helpers for `token_issuance_log`.

Every HSBIDAO issuance path commits an intent row before broadcasting. The
broadcast is the only step that cannot be rolled back, so a row that never
reaches SUCCESS is HELD at PENDING: it keeps blocking a re-issue of the same
thing until something proves the operation's fate. See THE REMIT in
hsbi_token_snapshot for what is allowed to constitute that proof.

rationale is the durable, never-rewritten reason an issuance happened (pik,
Pending Balance Conversion, Unit Conversion, Management). It is always written
by the code that initiates the issuance and is never inferred from the chain.
"""

from datetime import datetime, timezone

PENDING_TRX_PLACEHOLDER = "PENDING"
UNCAPTURED_TRX_PLACEHOLDER = "N/A"


def utcnow():
    return datetime.now(timezone.utc)


def issue_trx_id(tx):
    return tx.get("trx_id") or tx.get("transaction_id") or UNCAPTURED_TRX_PLACEHOLDER


def insert_pending_issuance(
    conn, recipient, units, rationale, source_trx_id=None, issued_at=None
):
    issued_at = issued_at or utcnow()
    result = conn.exec_driver_sql(
        """
        INSERT INTO token_issuance_log
            (trx_id, recipient, units, issued_at, status, error_message, rationale, source_trx_id)
        VALUES (%s, %s, %s, %s, 'PENDING', NULL, %s, %s)
        """,
        (PENDING_TRX_PLACEHOLDER, recipient, units, issued_at, rationale, source_trx_id),
    )
    return result.lastrowid


def mark_issuance_success(conn, log_id, trx_id):
    conn.exec_driver_sql(
        """
        UPDATE token_issuance_log
        SET status = 'SUCCESS', trx_id = %s, error_message = NULL
        WHERE id = %s
        """,
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
    failure here. The error is stored rather than acted on: it is the evidence
    fail_pending_with_proven_non_inclusion reads later, and failing a row that did
    reach the chain re-issues tokens that already exist.
    """
    conn.exec_driver_sql(
        "UPDATE token_issuance_log SET error_message = %s WHERE id = %s",
        (error_message, log_id),
    )


# Errors that prove the transaction was rejected during validation and therefore
# can never appear in a block. For these the usual "it might still have landed"
# caution does not apply, and holding the row PENDING is actively harmful: the row
# blocks its (recipient, rationale) in issue_balance_tokens, so the member stops
# being paid until reconciliation proves an absence that was certain all along.
#
# The bar for adding a marker here is that the node rejected the operation
# deterministically before inclusion. Anything ambiguous — timeouts, transport
# failures, dropped connections, unknown errors — must stay PENDING, because
# failing a row that did reach the chain re-issues tokens that already exist.
NEVER_REACHED_CHAIN_MARKERS = (
    # Hive caps an account at 5 custom_json operations per block. The 6th is
    # asserted away when the node applies it, so it is never included.
    "HIVE_CUSTOM_OP_BLOCK_LIMIT",
)


def never_reached_chain(error_message):
    """True when the broadcast error proves the operation was never included."""
    if not error_message:
        return False
    text = str(error_message)
    return any(marker in text for marker in NEVER_REACHED_CHAIN_MARKERS)


def record_broadcast_error(conn, log_id, error_message):
    """Record a raised broadcast against its write-ahead row.

    Fails the row outright when the error proves the operation never reached the
    chain, so the next cycle simply re-issues it; otherwise leaves it PENDING,
    where it blocks a re-issue until an operator settles it. Returns the
    resulting status.
    """
    if never_reached_chain(error_message):
        mark_issuance_failure(conn, log_id, error_message)
        return "FAILURE"
    record_pending_error(conn, log_id, error_message)
    return "PENDING"


def fail_pending_with_proven_non_inclusion(conn):
    """Fail every PENDING row whose recorded error already proves non-inclusion.

    record_pending_error stores the raised broadcast error on the row, so a row
    stuck from before record_broadcast_error existed still carries the proof of
    its own rejection in `error_message`. Reading it costs one read and a
    keyed UPDATE and no network, where discovering the same fact from chain
    history costs a scan whose reach has to be stretched over the row's whole age.

    This is the same decision record_broadcast_error makes at broadcast time,
    made late: the marker list is the single bar for both, so adding a marker
    also retroactively clears historical rows carrying that error. It is the only
    thing that clears a stuck row without an operator, which is why the bar for
    adding a marker is proof of rejection before inclusion and nothing weaker.

    The match is made in Python by never_reached_chain, not by SQL LIKE, so the
    two paths are literally the same predicate. A LIKE pattern built from a
    marker would also read `_` and `%` inside it as wildcards, matching messages
    record_broadcast_error would have left PENDING — and failing a row whose
    broadcast did reach the chain re-issues tokens that already exist.

    Rows are then updated by primary key. The alternative — one UPDATE with the
    predicate in its WHERE clause — has no index to use (`status` is not
    indexed on its own) and so takes next-key locks across the whole table
    while the unified webserver is reading it.

    Returns the number of rows failed.
    """
    if not NEVER_REACHED_CHAIN_MARKERS:
        return 0

    rows = conn.exec_driver_sql(
        """
        SELECT id, error_message FROM token_issuance_log
        WHERE status = 'PENDING' AND error_message IS NOT NULL
        """
    ).fetchall()

    doomed = [row[0] for row in rows if never_reached_chain(row[1])]
    if not doomed:
        return 0

    placeholders = ", ".join(["%s"] * len(doomed))
    result = conn.exec_driver_sql(
        f"""
        UPDATE token_issuance_log
        SET status = 'FAILURE'
        WHERE id IN ({placeholders})
        """,
        tuple(doomed),
    )
    return result.rowcount or len(doomed)


def has_issuance_for_source(conn, source_trx_id, rationale):
    """True when the origin transaction already has a live issuance row.

    PENDING (in flight / awaiting reconciliation) and SUCCESS rows both block a
    re-issue for the same source transaction; a FAILURE row (provably never on
    chain) does not, so a genuine retry stays possible.
    """
    count = conn.exec_driver_sql(
        """
        SELECT COUNT(*) FROM token_issuance_log
        WHERE source_trx_id = %s AND rationale = %s
          AND status IN ('PENDING', 'SUCCESS')
        """,
        (source_trx_id, rationale),
    ).scalar()
    return bool(count)


def log_issuance(
    conn,
    trx_id,
    recipient,
    units,
    status,
    rationale,
    error_message=None,
    source_trx_id=None,
    issued_at=None,
):
    """Append a terminal (SUCCESS/FAILURE) issuance row.

    Retained for legacy tests and historical compatibility; new issuance paths
    should use insert_pending_issuance followed by mark_issuance_success.
    """
    issued_at = issued_at or utcnow()
    conn.exec_driver_sql(
        """
        INSERT INTO token_issuance_log
            (trx_id, recipient, units, issued_at, status, error_message, rationale, source_trx_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (trx_id, recipient, units, issued_at, status, error_message, rationale, source_trx_id),
    )
