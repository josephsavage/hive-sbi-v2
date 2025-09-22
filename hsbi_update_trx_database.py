from hivesbi.settings import get_runtime
from hivesbi.storage import TrxDB


def run():
    rt = get_runtime()
    # Create storages
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
    print(f"hsbi_update_trx_database: the trx database has {n_records} records")
    print("hsbi_update_trx_database: Number of shares:")
    print(f"hsbi_update_trx_database: shares: {shares}")
    print("hsbi_update_trx_database: status:")
    for s in status:
        print(f"hsbi_update_trx_database: {status[s]} status entries with {s}")
    print("hsbi_update_trx_database: share_types:")
    for s in share_type:
        print(f"hsbi_update_trx_database: {share_type[s]} share_type entries with {s}")


if __name__ == "__main__":
    run()
