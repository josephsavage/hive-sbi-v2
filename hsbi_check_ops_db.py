import json
import time

from nectar.account import Account
from nectar.utils import formatTimeString

from hivesbi.settings import get_runtime, make_hive
from hivesbi.transfer_ops_storage import AccountTrx, TransferTrx


def run():
    start_prep_time = time.time()
    rt = get_runtime()
    cfg = rt["cfg"]
    db = rt["db"]
    accounts = rt["accounts"]

    hv = make_hive(cfg)
    # print(str(hv))

    print("hsbi_check_ops_db: Fetch new account history ops.")

    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
        if not accountTrx[account].exists_table():
            accountTrx[account].create_table()

    # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
    # stop_index = formatTimeString("2018-07-21T23:46:09")

    for account_name in accounts:
        if account_name != "steembasicincome":
            continue
        account = Account(account_name, blockchain_instance=hv)

        # Go trough all transfer ops
        cnt = 0

        start_index = accountTrx[account_name].get_latest_index()
        if start_index is not None:
            start_index = start_index["op_acc_index"] + 1
            print(f"hsbi_check_ops_db: account {account['name']} - {start_index}")
        else:
            start_index = 0
        start_index = 0
        data = []
        if account.virtual_op_count() > start_index:
            for op in account.history(start=start_index, use_block_num=False):
                virtual_op = op["virtual_op"]
                trx_in_block = op["trx_in_block"]
                if virtual_op > 0:
                    trx_in_block = -1
                d = {
                    "block": op["block"],
                    "op_acc_index": op["index"],
                    "op_acc_name": account["name"],
                    "trx_in_block": trx_in_block,
                    "op_in_trx": op["op_in_trx"],
                    "virtual_op": virtual_op,
                    "timestamp": formatTimeString(op["timestamp"]),
                    "type": op["type"],
                    "op_dict": json.dumps(op),
                }
                data.append(d)
                if cnt % 1000 == 0:
                    print(f"hsbi_check_ops_db: {op['timestamp']}")
                    accountTrx[account_name].add_batch(data)
                    data = []
                cnt += 1
            if len(data) > 0:
                print(f"hsbi_check_ops_db: {op['timestamp']}")
                accountTrx[account_name].add_batch(data)
                data = []
    for account_name in accounts:
        if account_name != "steembasicincome":
            continue
        account = Account(account_name, blockchain_instance=hv)

        # Go trough all transfer ops
        cnt = 0

        start_index = accountTrx[account_name].get_latest_index()
        if start_index is not None:
            start_index = start_index["op_acc_index"] + 1
            print("account %s - %d" % (account["name"], start_index))
        else:
            start_index = 0
        data = []
        if account.virtual_op_count() > start_index:
            for op in account.history(start=start_index, use_block_num=False):
                virtual_op = op["virtual_op"]
                trx_in_block = op["trx_in_block"]
                if virtual_op > 0:
                    trx_in_block = -1
                d = {
                    "block": op["block"],
                    "op_acc_index": op["index"],
                    "op_acc_name": account["name"],
                    "trx_in_block": trx_in_block,
                    "op_in_trx": op["op_in_trx"],
                    "virtual_op": virtual_op,
                    "timestamp": formatTimeString(op["timestamp"]),
                    "type": op["type"],
                    "op_dict": json.dumps(op),
                }
                data.append(d)
                if cnt % 1000 == 0:
                    print(op["timestamp"])
                    accountTrx[account_name].add_batch(data)
                    data = []
                cnt += 1
            if len(data) > 0:
                print(op["timestamp"])
                accountTrx[account_name].add_batch(data)
                data = []

    # Create keyStorage
    trxStorage = TransferTrx(db)

    if not trxStorage.exists_table():
        trxStorage.create_table()
    print(
        f"hsbi_check_ops_db: store ops script run {time.time() - start_prep_time:.2f} s"
    )


if __name__ == "__main__":
    run()
