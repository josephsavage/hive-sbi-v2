import json
import os
import time
from datetime import datetime, timezone

import dataset
from nectar import Steem
from nectar.account import Account
from nectar.amount import Amount
from nectar.nodelist import NodeList
from nectar.utils import formatTimeString

from steembi.member import Member
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import (
    AccountsDB,
    ConfigurationDB,
    KeysDB,
    MemberDB,
    TransactionMemoDB,
    TransactionOutDB,
    TrxDB,
)
from steembi.transfer_ops_storage import AccountTrx
from steembi.utils import ensure_timezone_aware

def run():
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]


    start_prep_time = time.time()
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    
    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()    
    other_accounts = accountStorage.get_transfer()    
    
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
    
    confStorage = ConfigurationDB(db2)
    conf_setup = confStorage.get()
    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    
    print("sbi_transfer: last_cycle: %s - %.2f min" % (formatTimeString(last_cycle), (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60))
    
    if last_cycle is not None and  (datetime.now(timezone.utc) - last_cycle).total_seconds() > 60 * share_cycle_min:    
    
        key_list = []
        print("Parse new transfers.")
        key = keyStorage.get("steembasicincome", "memo")
        if key is not None:
            key_list.append(key["wif"])
        #print(key_list)
        nodes = NodeList()
        try:
            nodes.update_nodes()
        except Exception:
            print("could not update nodes")    
        stm = Steem(keys=key_list, node=nodes.get_nodes(hive=hive_blockchain))
        # set_shared_steem_instance(stm)    
        
        # print("load member database")
        member_accounts = memberStorage.get_all_accounts()
        member_data = {}
        n_records = 0
        share_age_member = {}    
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))
        
        
        if True:
            print("delete from transaction_memo... ")
#            transactionStorage.delete_sender("dtube.rewards")
#            transactionStorage.delete_sender("reward.app")
#            transactionStorage.delete_to("sbi2")
#            transactionStorage.delete_to("sbi3")
#            transactionStorage.delete_to("sbi4")
#            transactionStorage.delete_to("sbi5")
#            transactionStorage.delete_to("sbi6")
#            transactionStorage.delete_to("sbi7")
#            transactionStorage.delete_to("sbi8")
#            transactionStorage.delete_to("sbi9")
#            transactionStorage.delete_to("sbi10")
            print("done.")
    
        stop_index = None
        # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
        # stop_index = formatTimeString("2018-07-21T23:46:09")    
    
        for account_name in accounts:
            if account_name == "steembasicincome":
                account_trx_name = "sbi"
            else:
                account_trx_name = account_name
            parse_vesting = (account_name == "steembasicincome")
            accountTrx[account_trx_name].db = dataset.connect(databaseConnector)
            account = Account(account_name, steem_instance=stm)
            # print(account["name"])
            pah = ParseAccountHist(account, "", trxStorage, transactionStorage, transactionOutStorage, member_data, memberStorage=memberStorage, steem_instance=stm)
            
            op_index = trxStorage.get_all_op_index(account["name"])
            
            if len(op_index) == 0:
                start_index = 0
                op_counter = 0
                start_index_offset = 0
            else:
                op = trxStorage.get(op_index[-1], account["name"])
                start_index = op["index"] + 1
                op_counter = op_index[-1] + 1
                if account_name == "steembasicincome":
                    start_index_offset = 316
                else:
                    start_index_offset = 0
    
            # print("start_index %d" % start_index)
            # ops = []
            # 
                
            ops = accountTrx[account_trx_name].get_all(op_types=["transfer", "delegate_vesting_shares"])
            if len(ops) == 0:
                continue
            
            if ops[-1]["op_acc_index"] < start_index - start_index_offset:
                continue
            for op in ops:
                if op["op_acc_index"] < start_index - start_index_offset:
                    continue
                if stop_index is not None and formatTimeString(op["timestamp"]) > stop_index:
                    continue
                json_op = json.loads(op["op_dict"])
                json_op["index"] = op["op_acc_index"] + start_index_offset
                if account_name != "steembasicincome" and json_op["type"] == "transfer":
                    if float(Amount(json_op["amount"], steem_instance=stm)) < 1:
                        continue
                    if json_op["memo"][:8] == 'https://':
                        continue
                    
                pah.parse_op(json_op, parse_vesting=parse_vesting)
    
    
        print("transfer script run %.2f s" % (time.time() - start_prep_time))

if __name__ == "__main__":
    run()
