import json
import os
import random
import time

import dataset
from nectar import Hive
from nectar.blockchain import Blockchain
from nectar.comment import Comment
from nectar.nodelist import NodeList
from nectar.utils import construct_authorperm

from hivesbi.member import Member
from hivesbi.storage import (
    AccountsDB,
    BlacklistDB,
    ConfigurationDB,
    KeysDB,
    MemberDB,
)
from hivesbi.transfer_ops_storage import PostsTrx
from hivesbi.utils import ensure_timezone_aware


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
        hive_blockchain = config_data["hive_blockchain"]

    start_prep_time = time.time()
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)
    blacklistStorage = BlacklistDB(db2)
    accStorage = AccountsDB(db2)
    keyStorage = KeysDB(db2)

    accounts = accStorage.get()

    blacklist = blacklistStorage.get()

    blacklist_tags = []
    for t in blacklist["tags"].split(","):
        blacklist_tags.append(t.strip())

    blacklist_apps = []
    for t in blacklist["apps"].split(","):
        blacklist_apps.append(t.strip())

    blacklist_body = []
    for t in blacklist["body"].split(","):
        blacklist_body.append(t.strip())

    conf_setup = confStorage.get()

    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    comment_vote_divider = conf_setup["comment_vote_divider"]
    comment_footer = conf_setup["comment_footer"]

    member_accounts = memberStorage.get_all_accounts()
    print(f"hsbi_stream_post_comment: {len(member_accounts)} members in list")

    nobroadcast = False
    # nobroadcast = True

    member_data = {}
    for m in member_accounts:
        member_data[m] = Member(memberStorage.get(m))

    postTrx = PostsTrx(db)

    print("hsbi_stream_post_comment: stream new posts")

    max_batch_size = 50
    threading = False
    nodes = NodeList()
    try:
        nodes.update_nodes()
    except Exception:
        print("hsbi_stream_post_comment: could not update nodes")

    keys = []
    account_list = []
    for acc in accounts:
        account_list.append(acc)
        keys.append(keyStorage.get(acc, "posting"))
    keys_list = []
    for k in keys:
        if k["key_type"] == "posting":
            keys_list.append(k["wif"].replace("\n", "").replace("\r", ""))
    node_list = nodes.get_nodes(hive=hive_blockchain)
    hv = Hive(
        node=node_list,
        keys=keys_list,
        num_retries=5,
        call_num_retries=3,
        timeout=15,
        nobroadcast=nobroadcast,
    )

    b = Blockchain(mode="irreversible", blockchain_instance=hv)
    print("hsbi_stream_post_comment: deleting old posts")
    # postTrx.delete_old_posts(1)
    start_block = b.get_current_block_num() - int(28800)
    stop_block = b.get_current_block_num()
    last_block_print = start_block

    latest_update = postTrx.get_latest_post()
    latest_block = postTrx.get_latest_block()
    if latest_block is not None and latest_block > start_block:
        latest_update_block = latest_block
    elif latest_block is not None and latest_block < start_block:
        latest_update_block = start_block
    elif latest_update is not None:
        latest_update_block = b.get_estimated_block_num(latest_update)
    else:
        latest_update_block = start_block
    print(
        "latest update %s - %d to %d"
        % (str(latest_update), latest_update_block, stop_block)
    )

    start_block = max([latest_update_block, start_block]) + 1
    if stop_block > start_block + 6000:
        stop_block = start_block + 6000
    cnt = 0
    posts_dict = {}
    changed_member_data = []
    for ops in b.stream(
        start=start_block,
        stop=stop_block,
        opNames=["comment"],
        max_batch_size=max_batch_size,
        threading=threading,
        thread_num=8,
    ):
        if ops["author"] not in member_accounts:
            continue
        if ops["block_num"] <= latest_update_block:
            continue
        if ops["block_num"] - last_block_print > 50:
            last_block_print = ops["block_num"]
            print(
                "blocks left %d - post found: %d"
                % (ops["block_num"] - stop_block, len(posts_dict))
            )
        authorperm = construct_authorperm(ops)
        c = None
        cnt = 0
        while c is None and cnt < 5:
            cnt += 1
            try:
                c = Comment(authorperm, blockchain_instance=hv)
            except Exception:
                c = None
                continue
        if c is None:
            continue
        main_post = c.is_main_post()
        if ops["author"] not in changed_member_data:
            changed_member_data.append(ops["author"])
        if main_post:
            if "last_update" in c:
                last_update = c["last_update"]
            else:
                last_update = c["updated"]
            if c["created"] == last_update:
                member_data[ops["author"]]["last_post"] = c["created"]
                member_data[ops["author"]]["comment_upvote"] = 0
        else:
            member_data[ops["author"]]["last_comment"] = c["created"]
            created_time = ensure_timezone_aware(c["created"])
            ops_time = ensure_timezone_aware(ops["timestamp"])
            if (
                "!sbi status" in c.body.lower()
                and abs((ops_time - created_time).total_seconds()) <= 30
            ):
                reply_body = "Hi @%s!\n\n" % ops["author"]
                reply_body += "* you have %d units and %d bonus units\n" % (
                    member_data[ops["author"]]["shares"],
                    member_data[ops["author"]]["bonus_shares"],
                )
                reply_body += "* your rshares balance is %d or %.3f $\n" % (
                    member_data[ops["author"]]["balance_rshares"],
                    hv.rshares_to_hbd(member_data[ops["author"]]["balance_rshares"]),
                )
                rshares = (
                    member_data[ops["author"]]["balance_rshares"] / comment_vote_divider
                )
                if rshares > minimum_vote_threshold:
                    reply_body += (
                        "* your next SBI upvote is predicted to be %.3f $\n"
                        % (hv.rshares_to_hbd(rshares))
                    )
                else:
                    reply_body += (
                        "* you need to wait until your upvote value (current value: %.3f $) is above %.3f $\n"
                        % (
                            hv.rshares_to_hbd(rshares),
                            hv.rshares_to_hbd(minimum_vote_threshold),
                        )
                    )
                if len(comment_footer) > 0:
                    reply_body += "<br>\n"
                    reply_body += comment_footer

                account_name = account_list[random.randint(0, len(account_list) - 1)]
                try:
                    print(
                        f"Replying to @{c['author']}/{c['permlink']} with account {account_name}"
                    )
                    c.reply(reply_body, author=account_name)
                    time.sleep(4)
                except Exception as e:
                    print(
                        f"hsbi_stream_post_comment: Error replying to status comment: {e}"
                    )
                    continue

        already_voted = False

        dt_created = c["created"]
        dt_created = dt_created.replace(tzinfo=None)
        skip = False
        if (
            "tags" in c and c["tags"] is not None and isinstance(c["tags"], list)
        ):  # ensure that tags is an array
            for tag in c["tags"]:
                if (
                    tag is not None
                    and isinstance(tag, str)
                    and tag.lower() in blacklist_tags
                ):
                    skip = True
        json_metadata = c.json_metadata
        if isinstance(json_metadata, str):
            try:
                json_metadata = json.loads(json_metadata)
            except Exception:
                json_metadata = {}
        for s in blacklist_body:
            if s in c.body.lower():
                skip = True

        vote_delay = member_data[ops["author"]]["upvote_delay"]
        if vote_delay is None:
            vote_delay = 300
        posts_dict[authorperm] = {
            "authorperm": authorperm,
            "author": ops["author"],
            "created": dt_created,
            "block": ops["block_num"],
            "main_post": main_post,
            "voted": already_voted,
            "skip": skip,
            "vote_delay": vote_delay,
        }

        if len(posts_dict) > 100:
            start_time = time.time()
            postTrx.add_batch(posts_dict)
            print(
                "Adding %d post took %.2f seconds"
                % (len(posts_dict), time.time() - start_time)
            )
            posts_dict = {}

        cnt += 1

    print("hsbi_stream_post_comment: write member database")
    member_data_list = []
    for m in changed_member_data:
        member_data_list.append(member_data[m])

    db2 = dataset.connect(databaseConnector2)
    memberStorage = MemberDB(db2)
    memberStorage.add_batch(member_data_list)
    member_data_list = []
    if len(posts_dict) > 0:
        start_time = time.time()
        postTrx.add_batch(posts_dict)
        print(
            "Adding %d post took %.2f seconds"
            % (len(posts_dict), time.time() - start_time)
        )
        posts_dict = {}

    print(
        f"hsbi_stream_post_comment: stream posts script run {time.time() - start_prep_time:.2f} s"
    )


if __name__ == "__main__":
    run()
