import json
import os
from datetime import datetime, timedelta, timezone

import dataset
from nectar import Hive
from nectar.block import Block
from nectar.blockchain import Blockchain
from nectar.comment import Comment
from nectar.nodelist import NodeList
from nectar.utils import addTzInfo, formatTimeString
from nectar.wallet import Wallet
from nectarbase.signedtransactions import Signed_Transaction
from nectargraphenebase.base58 import Base58

from hivesbi.member import Member
from hivesbi.storage import ConfigurationDB, MemberDB, TrxDB
from hivesbi.transfer_ops_storage import AccountTrx, MemberHistDB, TransferTrx
from hivesbi.utils import ensure_timezone_aware

if __name__ == "__main__":
    config_file = "config.json"
    if not os.path.isfile(config_file):
        accounts = [
            "steembasicincome",
            "sbi2",
            "sbi3",
            "sbi4",
            "sbi5",
            "sbi6",
            "sbi7",
            "sbi8",
            "sbi9",
        ]
        path = "E:\\sbi\\"
        database = "sbi_ops.sqlite"
        database_transfer = "sbi_transfer.sqlite"
        databaseConnector = None
        other_accounts = ["minnowbooster"]
        mgnt_shares = {"josephsavage": 3, "earthnation-bot": 1, "holger80": 1}
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        accounts = config_data["accounts"]
        path = config_data["path"]
        database = config_data["database"]
        database_transfer = config_data["database_transfer"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]

    db2 = dataset.connect(databaseConnector2)
    db = dataset.connect(databaseConnector)
    transferStorage = TransferTrx(db)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    accountStorage = MemberHistDB(db)
    confStorage = ConfigurationDB(db2)

    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)

    conf_setup = confStorage.get()

    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = ensure_timezone_aware(conf_setup["last_paid_post"])
    last_paid_comment = conf_setup["last_paid_comment"]

    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    comment_vote_divider = conf_setup["comment_vote_divider"]
    comment_vote_timeout_h = conf_setup["comment_vote_timeout_h"]

    print(
        "last_cycle: %s - %.2f min"
        % (
            formatTimeString(last_cycle),
            (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60,
        )
    )
    if True:
        last_cycle = datetime.now(timezone.utc) - timedelta(seconds=60 * 145)
        confStorage.update({"last_cycle": last_cycle})
        print("update member database")
        # memberStorage.wipe(True)
        member_accounts = memberStorage.get_all_accounts()
        data = trxStorage.get_all_data()

        # Update current node list from @fullnodeupdate
        nodes = NodeList()
        nodes.update_nodes()
        hv = Hive(node=nodes.get_nodes(hive=hive_blockchain))
        # hv = Hive()
        member_data = {}
        n_records = 0
        share_age_member = {}
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))

        if True:
            b = Blockchain(blockchain_instance=hv)
            wallet = Wallet(blockchain_instance=hv)

            for acc_name in accounts:
                print(acc_name)
                comments_transfer = []
                ops = accountTrx[acc_name].get_all(op_types=["transfer"])
                cnt = 0
                for o in ops:
                    cnt += 1
                    if cnt % 10 == 0:
                        print("%d/%d" % (cnt, len(ops)))
                    op = json.loads(o["op_dict"])
                    if op["memo"] == "":
                        continue
                    try:
                        c = Comment(op["memo"], blockchain_instance=hv)
                    except Exception:
                        continue
                    if c["author"] not in accounts:
                        continue
                    if c["authorperm"] not in comments_transfer:
                        comments_transfer.append(c["authorperm"])
                print("%d comments with transfer found" % len(comments_transfer))
                for authorperm in comments_transfer:
                    c = Comment(authorperm, blockchain_instance=hv)
                    print(c["authorperm"])
                    for vote in c["active_votes"]:
                        if vote["rshares"] == 0:
                            continue
                        if (
                            addTzInfo(datetime.now(timezone.utc))
                            - ensure_timezone_aware(vote["time"])
                        ).total_seconds() / 60 / 60 / 24 <= 7:
                            continue
                        if vote["voter"] not in member_data:
                            continue
                        if vote["rshares"] > 50000000:
                            try:
                                block_num = b.get_estimated_block_num(vote["time"])
                                current_block_num = b.get_current_block_num()
                                transaction = None
                                block_search_list = [
                                    0,
                                    1,
                                    -1,
                                    2,
                                    -2,
                                    3,
                                    -3,
                                    4,
                                    -4,
                                    5,
                                    -5,
                                ]
                                block_cnt = 0
                                while transaction is None and block_cnt < len(
                                    block_search_list
                                ):
                                    if (
                                        block_num + block_search_list[block_cnt]
                                        > current_block_num
                                    ):
                                        block_cnt += 1
                                        continue
                                    block = Block(
                                        block_num + block_search_list[block_cnt],
                                        blockchain_instance=hv,
                                    )
                                    for tt in block.transactions:
                                        for op in tt["operations"]:
                                            if (
                                                isinstance(op, dict)
                                                and op["type"][:4] == "vote"
                                            ):
                                                if (
                                                    op["value"]["voter"]
                                                    == vote["voter"]
                                                ):
                                                    transaction = tt
                                            elif (
                                                isinstance(op, list)
                                                and len(op) > 1
                                                and op[0][:4] == "vote"
                                            ):
                                                if op[1]["voter"] == vote["voter"]:
                                                    transaction = tt
                                    block_cnt += 1
                                vote_did_sign = True
                                key_accounts = []
                                if transaction is not None:
                                    signed_tx = Signed_Transaction(transaction)
                                    public_keys = []
                                    for key in signed_tx.verify(
                                        chain=hv.chain_params, recover_parameter=True
                                    ):
                                        public_keys.append(
                                            format(
                                                Base58(key, prefix=hv.prefix), hv.prefix
                                            )
                                        )

                                    empty_public_keys = []
                                    for key in public_keys:
                                        pubkey_account = wallet.getAccountFromPublicKey(
                                            key
                                        )
                                        if pubkey_account is None:
                                            empty_public_keys.append(key)
                                        else:
                                            key_accounts.append(pubkey_account)

                                for a in key_accounts:
                                    if vote["voter"] == a:
                                        continue
                                    if a not in ["quarry", "steemdunk"]:
                                        print(a)
                                    if a in [
                                        "smartsteem",
                                        "smartmarket",
                                        "minnowbooster",
                                    ]:
                                        vote_did_sign = False

                                if not vote_did_sign:
                                    continue
                            except Exception:
                                continue
                        if vote_did_sign:
                            continue
