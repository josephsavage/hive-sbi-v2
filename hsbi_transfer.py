import json
import time
from datetime import datetime, timezone

from nectar.account import Account
from nectar.utils import formatTimeString

from hivesbi.member import Member
from hivesbi.parse_hist_op import ParseAccountHist
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import (
    AuditDB,
    ConfigurationDB,
    KeysDB,
    MemberDB,
    TransactionMemoDB,
    TransactionOutDB,
    TrxDB,
)
from hivesbi.transfer_ops_storage import AccountTrx
from hivesbi.utils import ensure_timezone_aware


def run():
    start_prep_time = time.time()
    rt = get_runtime()
    cfg = rt["cfg"]
    db = rt["db"]
    db2 = rt["db2"]

    accounts = rt["accounts"]

    accountTrx = {}
    for account in accounts:
        if account == "steembasicincome":
            accountTrx["sbi"] = AccountTrx(db, "sbi")
        else:
            accountTrx[account] = AccountTrx(db, account)

    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    keyStorage = KeysDB(db2)
    transactionStorage = TransactionMemoDB(db2)
    transactionOutStorage = TransactionOutDB(db2)
    auditStorage = AuditDB(db2)

    confStorage = ConfigurationDB(db2)
    conf_setup = confStorage.get()
    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    # Calculate how many rshares correspond to 1 HBD using the rule:
    #   rshares_per_hbd = minimum_vote_threshold / 0.021
    # minimum_vote_threshold = conf_setup.get("minimum_vote_threshold", 0)

    print(
        f"hsbi_transfer: last_cycle: {formatTimeString(last_cycle)} - {(datetime.now(timezone.utc) - last_cycle).total_seconds() / 60:.2f} min"
    )

    if (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds()
        > 60 * share_cycle_min
    ):
        key_list = []
        print("hsbi_transfer: Parse new transfers.")
        key = keyStorage.get("steembasicincome", "memo")
        if key is not None:
            key_list.append(key["wif"])
        hv = make_hive(cfg, keys=key_list)
        # set_shared_blockchain_instance(hv)

        # Calculate how many rshares correspond to 1 HBD using Nectar:
        rshares_per_hbd = hv.hbd_to_rshares(1.0)

        # print("load member database")
        member_accounts = memberStorage.get_all_accounts()
        member_data = {}
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))

        stop_index = None
        # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
        # stop_index = formatTimeString("2018-07-21T23:46:09")

        for account_name in accounts:
            account_trx_key = "sbi" if account_name == "steembasicincome" else account_name
            account_trx = accountTrx[account_trx_key]
            parse_vesting = account_name == "steembasicincome"
            account = Account(account_name, blockchain_instance=hv)
            # print(account["name"])
            pah = ParseAccountHist(
                account,
                "",
                trxStorage,
                transactionStorage,
                transactionOutStorage,
                member_data,
                memberStorage=memberStorage,
                blockchain_instance=hv,
                auditStorage=auditStorage,
                rshares_per_hbd=rshares_per_hbd,
            )

            op_index = trxStorage.get_all_op_index(account["name"])

            if len(op_index) == 0:
                start_index = 0
                start_index_offset = 0
            else:
                op = trxStorage.get(op_index[-1], account["name"])
                start_index = op["index"] + 1
                if account_name == "steembasicincome":
                    start_index_offset = 316
                else:
                    start_index_offset = 0

            ops = account_trx.get_all(op_types=["transfer", "delegate_vesting_shares"])
            if len(ops) == 0:
                continue

            if ops[-1]["op_acc_index"] < start_index - start_index_offset:
                continue
            # Process operations in natural chronological order (op_acc_index)
            for op in ops:
                if op["op_acc_index"] < start_index - start_index_offset:
                    continue
                if (
                    stop_index is not None
                    and formatTimeString(op["timestamp"]) > stop_index
                ):
                    continue
                json_op = json.loads(op["op_dict"])
                json_op["index"] = op["op_acc_index"] + start_index_offset
                # Let ParseAccountHist.parse_op handle all transfer logic, including
                # URL promotion skips, point transfers, and small-transfer logging.
                pah.parse_op(json_op, parse_vesting=parse_vesting)

        print(
            f"hsbi_transfer: transfer script run {time.time() - start_prep_time:.2f} s"
        )


if __name__ == "__main__":
    run()
