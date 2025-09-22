from hivesbi.settings import get_runtime
from hivesbi.storage import TrxDB
from hivesbi.transfer_ops_storage import AccountTrx


def run():
    rt = get_runtime()
    db = rt["db"]
    accounts = rt["accounts"]
    stor = rt["storages"]
    trxStorage: TrxDB = stor["trx"]
    data = trxStorage.get_all_data()
    status = {}
    share_type = {}
    n_records = 0
    shares = 0

    for op in data:
        if op["status"] in status:
            status[op["status"]] += 1
        else:
            status[op["status"]] = 1
        if op["share_type"] in share_type:
            share_type[op["share_type"]] += 1
        else:
            share_type[op["share_type"]] = 1
        shares += op["shares"]
        n_records += 1
    print(f"hsbi_check_trx_database: the trx database has {n_records} records")
    print("hsbi_check_trx_database: Number of shares:")
    print(f"hsbi_check_trx_database: shares: {shares}")
    print("hsbi_check_trx_database: status:")
    for s in status:
        print(f"hsbi_check_trx_database: {status[s]} status entries with {s}")
    print("hsbi_check_trx_database: share_types:")
    for s in share_type:
        print(f"hsbi_check_trx_database: {share_type[s]} share_type entries with {s}")

    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
    for op in trxStorage.get_all_data_sorted():
        if op["source"] != "steembasicincome":
            continue


if __name__ == "__main__":
    run()
