import json

from hivesbi.settings import get_runtime
from hivesbi.transfer_ops_storage import AccountTrx


def run():
    rt = get_runtime()
    db = rt["db"]
    accounts = rt["accounts"]

    print("hsbi_compare_ops_db: Check account history ops.")
    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
        if not accountTrx[account].exists_table():
            accountTrx[account].create_table()
    # temp
    accountTrx["sbi"] = AccountTrx(db, "sbi")

    # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
    # stop_index = formatTimeString("2018-07-21T23:46:09")

    ops1 = accountTrx["steembasicincome"].get_all(
        op_types=["transfer", "delegate_vesting_shares"]
    )

    ops2 = accountTrx["sbi"].get_all(op_types=["transfer", "delegate_vesting_shares"])
    print(f"hsbi_compare_ops_db: ops loaded: length: {len(ops1)} - {len(ops2)}")

    index = 0
    while index < len(ops1) and index < len(ops2):
        op1 = ops1[index]
        op2 = ops2[index]

        dict1 = json.loads(op1["op_dict"])
        dict2 = json.loads(op2["op_dict"])
        if dict1["timestamp"] != dict2["timestamp"]:
            print(f"hsbi_compare_ops_db: {dict1['timestamp']} - {dict2['timestamp']}")
            print(f"hsbi_compare_ops_db: block: {op1['block']} - {op2['block']}")
            print(
                f"hsbi_compare_ops_db: index: {op1['op_acc_index']} - {op2['op_acc_index']}"
            )
        index += 1


if __name__ == "__main__":
    run()
