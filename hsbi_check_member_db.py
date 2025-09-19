import json
import os

import dataset
from nectar import Hive
from nectar.nodelist import NodeList

from hivesbi.storage import AccountsDB, ConfigurationDB, MemberDB, TrxDB

if __name__ == "__main__":
    config_file = "config.json"
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]

    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)

    accStorage = AccountsDB(db2)
    accounts = accStorage.get()
    other_accounts = accStorage.get_transfer()

    hp_share_ratio = confStorage.get()["sp_share_ratio"]

    nodes = NodeList()
    try:
        nodes.update_nodes()
    except Exception:
        print("hsbi_check_member_db: could not update nodes")
    hv = Hive(node=nodes.get_nodes(hive=hive_blockchain))

    # Update current node list from @fullnodeupdate
    print("hsbi_check_member_db: check member database")
    # memberStorage.wipe(True)
    member_accounts = memberStorage.get_all_accounts()
    data = trxStorage.get_all_data()

    missing_accounts = []
    member_data = {}
    aborted = False
    for m in member_accounts:
        member_data[m] = memberStorage.get(m)

    shares = 0
    bonus_shares = 0
    balance_rshares = 0
    for m in member_data:
        shares += member_data[m]["shares"]
        bonus_shares += member_data[m]["bonus_shares"]
        balance_rshares += member_data[m]["balance_rshares"]

    print(f"hsbi_check_member_db: units: {shares}")
    print(f"hsbi_check_member_db: bonus units: {bonus_shares}")
    print(f"hsbi_check_member_db: total units: {shares + bonus_shares}")
    print("hsbi_check_member_db: ----------")
    print(f"hsbi_check_member_db: balance_rshares: {balance_rshares}")
    print(
        f"hsbi_check_member_db: balance_rshares: {hv.rshares_to_hbd(balance_rshares):.3f} $"
    )
    if len(missing_accounts) > 0:
        print(f"hsbi_check_member_db: {len(missing_accounts)} not existing accounts: ")
        print(missing_accounts)
