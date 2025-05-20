import json
import os

import dataset
from nectar import Steem
from nectar.nodelist import NodeList

from steembi.storage import MemberDB, TrxDB

if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        print(config_data)
        accounts = config_data["accounts"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]
        

    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)

    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    try:
        nodes.update_nodes()
    except Exception:
        print("could not update nodes")       
    stm = Steem(node=nodes.get_nodes(hive=hive_blockchain))
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
    print("the trx database has %d records" % (n_records))
    print("Number of shares:")
    print("shares: %d" % shares)
    print("status:")
    for s in status:
        print("%d status entries with %s" % (status[s], s))
    print("share_types:")
    for s in share_type:
        print("%d share_type entries with %s" % (share_type[s], s))