# This Python file uses the following encoding: utf-8
"""
usage: hsbi_replay.py [-h] [--since-days SINCE_DAYS] [--sender SENDER]
                      [--to TO_ACCOUNT] [--trx-id TRX_ID] [--limit LIMIT]
                      [--dry-run]

Reprocess recent point transfers from transaction_memo

options:
  -h, --help            show this help message and exit
  --since-days SINCE_DAYS
                        Look back this many days (default: 10)
  --sender SENDER       Only reprocess entries from this sender
  --to TO_ACCOUNT       Only entries sent to this account (default:
                        steembasicincome)
  --trx-id TRX_ID       Only reprocess a specific trx_id
  --limit LIMIT         Optional cap on number of records reprocessed
  --dry-run             Do not persist or issue anything; just log
"""

import argparse
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from nectar.account import Account

from hivesbi.member import Member
from hivesbi.parse_hist_op import ParseAccountHist
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import (
    AuditDB,
    MemberDB,
    TransactionMemoDB,
    TransactionOutDB,
    TrxDB,
)
from hivesbi.utils import ensure_timezone_aware


def _iter_recent_point_like_memos(
    tx_memo_db: TransactionMemoDB,
    since_dt: datetime,
    to_account: Optional[str] = "steembasicincome",
    sender: Optional[str] = None,
    trx_id: Optional[str] = None,
) -> Iterable[dict]:
    """Yield transaction_memo rows from the last N days that look like unit/point transfers.

    Filters:
    - timestamp >= since_dt
    - optional to-account match (default steembasicincome)
    - optional sender match
    - optional trx_id match
    - asset filters applied by caller when reconstructing op
    """
    for row in tx_memo_db.get_all():
        # Row fields expected: id, sender, to, memo, encrypted, referenced_accounts,
        # amount (float), amount_symbol ("HBD"/"HIVE"), timestamp (datetime)
        ts = ensure_timezone_aware(row.get("timestamp"))
        if ts is None or ts < since_dt:
            continue
        if to_account and row.get("to") != to_account:
            continue
        if sender and row.get("sender") != sender:
            continue
        if trx_id and row.get("trx_id") != trx_id:
            continue
        yield row


def _reconstruct_op_from_row(row: dict) -> dict:
    amount_val = float(row.get("amount", 0.0))
    symbol = row.get("amount_symbol", "HBD")
    amount_str = f"{amount_val:.3f} {symbol}"
    op = {
        "type": "transfer",
        "from": row.get("sender"),
        "to": row.get("to"),
        "amount": amount_str,
        "memo": row.get("memo", ""),
        "timestamp": ensure_timezone_aware(row.get("timestamp"))
        or datetime.now(timezone.utc),
        # Use transaction_memo.id as stable index for TrxDB upsert idempotency
        "index": row.get("id", 0),
    }
    # Optional pass-through
    if "trx_id" in row:
        op["trx_id"] = row["trx_id"]
    if "op_acc_index" in row:
        op["op_acc_index"] = row["op_acc_index"]
    return op


def main():
    parser = argparse.ArgumentParser(
        description="Reprocess recent point transfers from transaction_memo"
    )
    parser.add_argument(
        "--since-days",
        type=int,
        default=10,
        help="Look back this many days (default: 10)",
    )
    parser.add_argument(
        "--sender",
        type=str,
        default=None,
        help="Only reprocess entries from this sender",
    )
    parser.add_argument(
        "--to",
        dest="to_account",
        type=str,
        default="steembasicincome",
        help="Only entries sent to this account (default: steembasicincome)",
    )
    parser.add_argument(
        "--trx-id", type=str, default=None, help="Only reprocess a specific trx_id"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional cap on number of records reprocessed",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not persist or issue anything; just log",
    )

    args = parser.parse_args()

    rt = get_runtime()
    cfg = rt["cfg"]
    db = rt["db"]
    db2 = rt["db2"]

    # Storages
    trx_db = TrxDB(db)
    tx_memo_db = TransactionMemoDB(db2)
    tx_out_db = TransactionOutDB(db2)
    member_db = MemberDB(db2)
    audit_db = AuditDB(db2)

    # Build member_data
    member_data = {}
    for name in member_db.get_all_accounts():
        row = member_db.get(name)
        if row:
            member_data[name] = Member(row)

    # Hive instance
    hv = make_hive(cfg)

    # Parser instance for SBI account
    sbi_account = Account(args.to_account, blockchain_instance=hv)
    pah = ParseAccountHist(
        sbi_account,
        path="",
        trxStorage=trx_db,
        transactionStorage=tx_memo_db,
        transactionOutStorage=tx_out_db,
        member_data=member_data,
        memberStorage=member_db,
        blockchain_instance=hv,
        auditStorage=audit_db,
        rshares_per_hbd=rt.get("minimum_vote_threshold", 1) / 0.021
        if rt.get("minimum_vote_threshold", 0)
        else 1,
    )

    since_dt = datetime.now(timezone.utc) - timedelta(days=args.since_days)

    processed = 0
    skipped = 0
    failed = 0

    for row in _iter_recent_point_like_memos(
        tx_memo_db,
        since_dt,
        to_account=args.to_account,
        sender=args.sender,
        trx_id=args.trx_id,
    ):
        # Asset gating consistent with _handle_point_transfer usage
        amt = float(row.get("amount", 0.0))
        sym = row.get("amount_symbol", "")
        if sym == "HBD":
            if amt < 0.005:
                skipped += 1
                continue
        elif sym == "HIVE":
            if not (0.005 <= amt < 1):
                skipped += 1
                continue
        else:
            skipped += 1
            continue

        op = _reconstruct_op_from_row(row)

        if args.dry_run:
            print(
                f"[DryRun] Would reprocess id={row.get('id')} sender={row.get('sender')} amount={op['amount']} memo={row.get('memo')!r}"
            )
            processed += 1
        else:
            try:
                # Use the normal parse path so transfer records and fallbacks are handled identically
                pah.parse_op(op, parse_vesting=False)
                processed += 1
            except Exception as exc:
                failed += 1
                print(
                    f"[Error] Failed to reprocess id={row.get('id')} trx_id={row.get('trx_id')} sender={row.get('sender')}: {exc}"
                )

        if args.limit and processed >= args.limit:
            break

    print(
        f"Reprocess summary: processed={processed} skipped={skipped} failed={failed} since={since_dt.isoformat()} to={args.to_account}"
    )


if __name__ == "__main__":
    main()
