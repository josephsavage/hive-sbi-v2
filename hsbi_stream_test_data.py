from nectar.account import Account

from hivesbi.settings import get_runtime, make_hive
from hivesbi.transfer_ops_storage import AccountTrx


def run():
    rt = get_runtime()
    cfg = rt["cfg"]
    db = rt["db"]
    accounts = rt["accounts"]

    hv = make_hive(cfg)
    print(f"hsbi_stream_test_data: {hv}")

    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)

        if not accountTrx[account].exists_table():
            accountTrx[account].create_table()

    for account_name in accounts:
        account = Account(account_name, blockchain_instance=hv)
        print(f"hsbi_stream_test_data: account {account['name']}")
        # Go trough all transfer ops
        ops = accountTrx[account_name].get_all()
        last_op_index = -1
        for op in ops:
            if op["op_acc_index"] - last_op_index != 1:
                print(
                    f"hsbi_stream_test_data: {account_name} - has missing ops {op['op_acc_index']} - {last_op_index} != 1"
                )
            else:
                last_op_index = op["op_acc_index"]
                continue


if __name__ == "__main__":
    run()
