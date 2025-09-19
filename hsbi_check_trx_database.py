import json
import os

import dataset

from hivesbi.storage import MemberDB, TrxDB
from hivesbi.transfer_ops_storage import AccountTrx

if __name__ == "__main__":
    config_file = "config.json"
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        print(f"hsbi_check_trx_database: {config_data}")
        accounts = config_data["accounts"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)

    # Update current node list from @fullnodeupdate
    # nodes = NodeList()
    # nodes.update_nodes()
    # hv = Hive(node=nodes.get_nodes())
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
    sbi_ops = accountTrx["steembasicincome"].get_all()
    last_index = -1
    for op in trxStorage.get_all_data_sorted():
        if op["source"] != "steembasicincome":
            continue
        if op["index"] - last_index:
            start_index = last_index
