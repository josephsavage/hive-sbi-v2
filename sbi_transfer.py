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
    AuditDB,
)
from steembi.transfer_ops_storage import AccountTrx
from steembi.utils import ensure_timezone_aware


def add_audit_log(
    auditStorage, account, value_type, old_value, new_value, reason, related_trx_id=None
):
    if old_value == new_value:
        return
    audit_log = {
        "account": account,
        "value_type": value_type,
        "old_value": old_value,
        "new_value": new_value,
        "change_amount": new_value - old_value,
        "timestamp": datetime.now(timezone.utc),
        "reason": reason,
        "related_trx_id": related_trx_id,
    }
    auditStorage.add(audit_log)


def handle_point_transfer(
    op, member_data, memberStorage, stm, auditStorage, trxStorage, rshares_per_hbd
):
    amount_obj = Amount(op["amount"], steem_instance=stm)
    amount = float(amount_obj)
    sender = op["from"]
    memo = op["memo"]

    if memo.startswith("@"):
        nominee = memo[1:]
    else:
        nominee = memo

    if sender not in member_data:
        return
    if nominee not in member_data:
        return

    sender_member = member_data[sender]
    nominee_member = member_data[nominee]

    if amount_obj.symbol == "HBD":
        old_sender_shares = sender_member.get("shares", 0)
        old_nominee_shares = nominee_member.get("shares", 0)
        units = int(amount * 1000)

        if old_sender_shares < units:
            units = old_sender_shares

        if units < 0:
            return

        if "shares" not in sender_member:
            sender_member["shares"] = 0
        if "shares" not in nominee_member:
            nominee_member["shares"] = 0

        sender_member["shares"] -= units
        nominee_member["shares"] += units

        memberStorage.update(sender_member)
        memberStorage.update(nominee_member)

        add_audit_log(
            auditStorage,
            sender,
            "shares",
            old_sender_shares,
            sender_member["shares"],
            f"Transferred {units} HSBI units to {nominee}",
            op["trx_id"],
        )
        add_audit_log(
            auditStorage,
            nominee,
            "shares",
            old_nominee_shares,
            nominee_member["shares"],
            f"Received {units} HSBI units from {sender}",
            op["trx_id"],
        )

        print(f"Transferred {units} units from {sender} to {nominee}")

        # ------------------------------------------------------------
        # Log the transfer in the trx table so that it shows up in
        # standard share-transfer reports, similar to how transfers
        # are logged in ParseAccountHist.
        # ------------------------------------------------------------
        try:
            index = op.get("op_acc_index", 0)
        except AttributeError:
            index = 0
            
        # Generate a unique index if the original is 0 to avoid primary key conflicts
        if index == 0:
            # Use timestamp as part of the unique index
            timestamp_obj = op.get("timestamp")
            if timestamp_obj is None:
                timestamp_obj = datetime.now(timezone.utc)
            if isinstance(timestamp_obj, str):
                timestamp_obj = datetime.fromisoformat(timestamp_obj.replace('Z', '+00:00'))
            # Create a unique index based on timestamp microseconds
            index = int(timestamp_obj.timestamp() * 1000000) % 1000000000
        # Build a common sponsee json string as used elsewhere in the
        # code-base ( {account: shares} )
        sponsee_json = json.dumps({nominee: units})
        # Fallback to current UTC timestamp if the op does not provide
        # one (should normally be available)
        timestamp = op.get("timestamp")
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        # Ensure timestamp string format matches other entries
        timestamp_str = (
            formatTimeString(timestamp) if not isinstance(timestamp, str) else timestamp
        )
        # Sender (negative shares)
        data_sender = {
            "index": index,
            "source": "member_transfer",
            "memo": f"Transfer to {nominee}",
            "account": sender,
            "sponsor": sender,
            "sponsee": sponsee_json,
            "shares": -units,
            "vests": 0.0,
            "timestamp": timestamp_str,
            "status": "Valid",
            "share_type": "Transfer",
        }
        # Nominee (positive shares)
        data_nominee = {
            "index": index,
            "source": "member_transfer",
            "memo": f"Received from {sender}",
            "account": nominee,
            "sponsor": sender,
            "sponsee": sponsee_json,
            "shares": units,
            "vests": 0.0,
            "timestamp": timestamp_str,
            "status": "Valid",
            "share_type": "Transfer",
        }
        trxStorage.add(data_sender)
        trxStorage.add(data_nominee)
    else:
        # Convert the micro amount to an HBD-equivalent value and then to rshares.
        # 1 HBD worth of rshares = rshares_per_hbd (minimum_vote_threshold / 0.021)
        old_sender_rshares = sender_member["balance_rshares"]
        old_nominee_rshares = nominee_member["balance_rshares"]

        hbd_equiv = amount * 1000  # micro-amount to HBD equivalent (e.g. 0.005 -> 5)
        points = int(hbd_equiv * rshares_per_hbd)

        if old_sender_rshares < points:
            points = old_sender_rshares

        if points <= 0:
            return

        sender_member["balance_rshares"] -= points
        nominee_member["balance_rshares"] += points

        memberStorage.update(sender_member)
        memberStorage.update(nominee_member)

        add_audit_log(
            auditStorage,
            sender,
            "balance_rshares",
            old_sender_rshares,
            sender_member["balance_rshares"],
            f"Transferred {points} rshares to {nominee}",
            op["trx_id"],
        )
        add_audit_log(
            auditStorage,
            nominee,
            "balance_rshares",
            old_nominee_rshares,
            nominee_member["balance_rshares"],
            f"Received {points} rshares from {sender}",
            op["trx_id"],
        )

        print(f"Transferred {points} rshares from {sender} to {nominee}")


def run():
    config_file = "config.json"
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
    auditStorage = AuditDB(db2)

    confStorage = ConfigurationDB(db2)
    conf_setup = confStorage.get()
    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    # Calculate how many rshares correspond to 1 HBD using the rule:
    #   rshares_per_hbd = minimum_vote_threshold / 0.021
    minimum_vote_threshold = conf_setup.get("minimum_vote_threshold", 0)
    if minimum_vote_threshold == 0:
        # Fallback: default factor of 1 to avoid ZeroDivision / missing config
        rshares_per_hbd = 1
    else:
        rshares_per_hbd = minimum_vote_threshold / 0.021

    print(
        "sbi_transfer: last_cycle: %s - %.2f min"
        % (
            formatTimeString(last_cycle),
            (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60,
        )
    )

    if (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds()
        > 60 * share_cycle_min
    ):
        key_list = []
        print("Parse new transfers.")
        key = keyStorage.get("steembasicincome", "memo")
        if key is not None:
            key_list.append(key["wif"])
        # print(key_list)
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
            parse_vesting = account_name == "steembasicincome"
            accountTrx[account_trx_name].db = dataset.connect(databaseConnector)
            account = Account(account_name, steem_instance=stm)
            # print(account["name"])
            pah = ParseAccountHist(
                account,
                "",
                trxStorage,
                transactionStorage,
                transactionOutStorage,
                member_data,
                memberStorage=memberStorage,
                steem_instance=stm,
            )

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

            ops = accountTrx[account_trx_name].get_all(
                op_types=["transfer", "delegate_vesting_shares"]
            )
            if len(ops) == 0:
                continue

            if ops[-1]["op_acc_index"] < start_index - start_index_offset:
                continue
            # Sort operations by amount (descending) so that larger transfers are handled first.
            ops_sorted = sorted(
                ops,
                key=lambda o: float(o.get("amount", 0)),
                reverse=True,
            )
            for op in ops_sorted:
                if op["op_acc_index"] < start_index - start_index_offset:
                    continue
                if (
                    stop_index is not None
                    and formatTimeString(op["timestamp"]) > stop_index
                ):
                    continue
                json_op = json.loads(op["op_dict"])
                json_op["index"] = op["op_acc_index"] + start_index_offset
                if json_op["type"] == "transfer":
                    amount = float(Amount(json_op["amount"], steem_instance=stm))
                    if account_name == "steembasicincome":
                        # Skip micro transfers below the minimum threshold
                        if amount < 0.005:
                            continue
                        # Handle point transfers between 0.005 and 1 HIVE/HBD
                        if amount < 1:
                            handle_point_transfer(
                                json_op,
                                member_data,
                                memberStorage,
                                stm,
                                auditStorage,
                                trxStorage,
                                rshares_per_hbd,
                            )
                            continue
                        # Skip large transfers that are purely URL promotions
                        if json_op["memo"][:8] == "https://":
                            continue
                    else:
                        # Skip transfers below 1 HIVE/HBD
                        if amount < 1:
                            continue
                        # Skip URL promotions
                        if json_op["memo"][:8] == "https://":
                            continue

                pah.parse_op(json_op, parse_vesting=parse_vesting)

        print("transfer script run %.2f s" % (time.time() - start_prep_time))


if __name__ == "__main__":
    run()
