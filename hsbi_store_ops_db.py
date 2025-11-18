import json
import time
from datetime import datetime, timezone

from nectar.account import Account
from nectar.amount import Amount
from nectar.blockchain import Blockchain
from nectar.utils import formatTimeString

from hivesbi.settings import get_runtime, make_hive
from hivesbi.transfer_ops_storage import AccountTrx
from hivesbi.utils import ensure_timezone_aware
from hivesbi.storage import (
    AuditDB,
    ConfigurationDB,
    KeysDB,
    MemberDB,
    TransactionMemoDB,
    TransactionOutDB,
    TrxDB,
)


def get_account_trx_data(account, start_block, start_index):
    # Go trough all transfer ops
    if start_block is not None:
        trx_in_block = start_block["trx_in_block"]
        op_in_trx = start_block["op_in_trx"]
        virtual_op = start_block["virtual_op"]
        start_block = start_block["block"]

        print(f"hsbi_store_ops_db: account {account['name']} - {start_block}")
    else:
        start_block = 0
        trx_in_block = 0
        op_in_trx = 0
        virtual_op = 0

    if start_index is not None:
        start_index = start_index["op_acc_index"] + 1
        # print("account %s - %d" % (account["name"], start_index))
    else:
        start_index = 0

    data = []
    last_block = 0
    last_trx = trx_in_block
    for op in account.history(start=start_block - 5, use_block_num=True):
        if op["block"] < start_block:
            # last_block = op["block"]
            continue
        elif op["block"] == start_block:
            if op["virtual_op"] == 0:
                if op["trx_in_block"] < trx_in_block:
                    last_trx = op["trx_in_block"]
                    continue
                if op["op_in_trx"] <= op_in_trx and (
                    trx_in_block != last_trx or last_block == 0
                ):
                    continue
            else:
                if op["virtual_op"] <= virtual_op and (trx_in_block == last_trx):
                    continue
        start_block = op["block"]
        virtual_op = op["virtual_op"]
        trx_in_block = op["trx_in_block"]

        if trx_in_block != last_trx or op["block"] != last_block:
            op_in_trx = op["op_in_trx"]
        else:
            op_in_trx += 1
        if virtual_op > 0:
            op_in_trx = 0
            if trx_in_block > 255:
                trx_in_block = 0

        d = {
            "block": op["block"],
            "op_acc_index": start_index,
            "op_acc_name": account["name"],
            "trx_in_block": trx_in_block,
            "op_in_trx": op_in_trx,
            "virtual_op": virtual_op,
            "timestamp": formatTimeString(op["timestamp"]),
            "type": op["type"],
            "op_dict": json.dumps(op),
        }
        # op_in_trx += 1
        start_index += 1
        last_block = op["block"]
        last_trx = trx_in_block
        data.append(d)
    return data


def get_account_trx_age_data(account, start_index, hv):
    if start_index is not None:
        start_index = start_index["op_acc_index"] + 1
        print(f"hsbi_e_ops_db: account {account['name']} - {start_index}")

    data = []
    for op in account.hiy(
        start=start_index, use_block_num=False, only_ops=["transfer"]
    ):
        amount = Amount(op["amount"], blockchain_instance=hv)
        virtual_op = op["virtual_op"]
        trx_in_block = op["trx_in_block"]
        if virtual_op > 0:
            trx_in_block = -1
        memo = ascii(op["memo"])
        d = {
            "block": op["block"],
            "op_acc_index": op["index"],
            "op_acc_name": account["name"],
            "trx_in_block": trx_in_block,
            "op_in_trx": op["op_in_trx"],
            "virtual_op": virtual_op,
            "timestamp": formatTimeString(op["timestamp"]),
            "from": op["from"],
            "to": op["to"],
            "amount": amount.amount,
            "amount_symbol": amount.symbol,
            "memo": memo,
            "op_type": op["type"],
        }
        data.append(d)
    return data


def run():
    start_prep_time = time.time()
    rt = get_runtime()
    stor = rt["storages"]
    cfg = rt["cfg"]
    db = rt["db"]
    db2 = rt["db2"]
    accounts = rt["accounts"]
    confage = ConfigurationDB(db2)
    confage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()

    if db2 is not None:
        with db2.engine.begin() as conn:
            result = conn.exec_driver_sql(
                "SELECT MAX(mana_pct) AS max_mana_pct FROM accounts"
            ).fetchone()
            max_mana_pct = result.max_mana_pct if result and result.max_mana_pct else 0
            print("Fetching max VP level:", max_mana_pct)

    mana_threshold = conf_setup.get("mana_pct_target", 0)
    max_mana_threshold = mana_threshold * 1.05
    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]

    print(
        f"hsbi_store_ops_db: last_cycle: {formatTimeString(last_cycle)} - {(datetime.now(timezone.utc) - last_cycle).total_seconds() / 60:.2f} min"
    )

    if (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds()
        > 60 * share_cycle_min
    ):
        hv = make_hive(cfg)
        print(f"hsbi_store_ops_db: {hv}")

        print("hsbi_store_ops_db: Fetch new account history ops.")

        _blockchain = Blockchain(blockchain_instance=hv)

        accountTrx = {}
        for account in accounts:
            if account == "steembasicincome":
                accountTrx["sbi"] = AccountTrx(db, "sbi")
            else:
                accountTrx[account] = AccountTrx(db, account)

        # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
        # stop_index = formatTimeString("2018-07-21T23:46:09")

        for account_name in accounts:
            if account_name == "steembasicincome":
                account = Account(account_name, blockchain_instance=hv)
                account_name = "sbi"
            else:
                account = Account(account_name, blockchain_instance=hv)
            start_block = accountTrx[account_name].get_latest_block()
            start_index = accountTrx[account_name].get_latest_index()

            data = get_account_trx_data(account, start_block, start_index)

            data_batch = []
            for cnt in range(0, len(data)):
                data_batch.append(data[cnt])
                if cnt % 1000 == 0:
                    accountTrx[account_name].add_batch(data_batch)
                    data_batch = []
            if len(data_batch) > 0:
                accountTrx[account_name].add_batch(data_batch)
                data_batch = []

        print(
            f"hsbi_store_ops_db: store ops script run {time.time() - start_prep_time:.2f} s"
        )


if __name__ == "__main__":
    run()
