import json
import os
import time
from datetime import datetime, timezone

import dataset
from nectar import Hive
from nectar.account import Account
from nectar.comment import Comment
from nectar.nodelist import NodeList
from nectar.utils import (
    addTzInfo,
    formatTimeString,
)
from nectar.vote import ActiveVotes

from hivesbi.member import Member
from hivesbi.storage import (
    AccountsDB,
    ConfigurationDB,
    MemberDB,
)
from hivesbi.transfer_ops_storage import AccountTrx
from hivesbi.utils import ensure_timezone_aware


def increment_rshares(member_data, vote, rshares):
    member_data[vote["voter"]]["earned_rshares"] += rshares
    member_data[vote["voter"]]["curation_rshares"] += rshares
    member_data[vote["voter"]]["balance_rshares"] += rshares


def update_account(
    account,
    new_paid_post,
    new_paid_comment,
    conf_setup,
    accounts_data,
    hv,
    accountTrx,
    member_data,
    rshares_per_cycle,
    upvote_multiplier,
    upvote_multiplier_adjusted,
    hv2,
):
    last_paid_post = ensure_timezone_aware(conf_setup["last_paid_post"])
    last_paid_comment = ensure_timezone_aware(conf_setup["last_paid_comment"])

    if accounts_data[account]["last_paid_comment"] is not None:
        last_paid_comment = accounts_data[account]["last_paid_comment"]
    if accounts_data[account]["last_paid_post"] is not None:
        last_paid_post = accounts_data[account]["last_paid_post"]

    account = Account(account, blockchain_instance=hv)
    if last_paid_post < last_paid_comment:
        oldest_timestamp = last_paid_post
    else:
        oldest_timestamp = last_paid_comment
    if account["name"] == "steembasicincome":
        ops = accountTrx["sbi"].get_newest(
            oldest_timestamp, op_types=["comment"], limit=500
        )
    else:
        ops = accountTrx[account["name"]].get_newest(
            oldest_timestamp, op_types=["comment"], limit=50
        )
    blog = []
    posts = []
    for op in ops[::-1]:
        try:
            comment = json.loads(op["op_dict"])
            created = formatTimeString(comment["timestamp"])
        except Exception:
            op_dict = op["op_dict"]
            comment = json.loads(op_dict[: op_dict.find("body") - 3] + "}")
        try:
            comment = Comment(comment, blockchain_instance=hv)
            comment.refresh()
            created = comment["created"]
        except Exception:
            continue
        if comment.is_pending():
            continue
        if comment["author"] != account["name"]:
            continue

        if comment["parent_author"] == "" and created > addTzInfo(last_paid_post):
            print(f"hsbi_update_curation_rshares: add post {comment['authorperm']}")
            blog.append(comment["authorperm"])
        elif comment["parent_author"] != "" and created > addTzInfo(last_paid_comment):
            print(f"hsbi_update_curation_rshares: add comment {comment['authorperm']}")
            posts.append(comment["authorperm"])

    post_rshares = 0
    for authorperm in blog:
        post = Comment(authorperm, blockchain_instance=hv)
        print(f"hsbi_update_curation_rshares: Checking post {post['authorperm']}")
        if post["created"] > addTzInfo(new_paid_post):
            new_paid_post = post["created"].replace(tzinfo=None)
        last_paid_post = post["created"].replace(tzinfo=None)
        all_votes = ActiveVotes(post["authorperm"], blockchain_instance=hv2)
        for vote in all_votes:
            if vote["voter"] in member_data:
                if member_data[vote["voter"]]["shares"] <= 0:
                    continue
                if account["name"] == "steembasicincome":
                    rshares = vote["rshares"] * upvote_multiplier
                    if rshares < rshares_per_cycle:
                        rshares = rshares_per_cycle
                else:
                    rshares = (
                        vote["rshares"] * upvote_multiplier * upvote_multiplier_adjusted
                    )

                increment_rshares(member_data, vote, rshares)
                post_rshares += rshares

    comment_rshares = 0
    for authorperm in posts:
        post = Comment(authorperm, blockchain_instance=hv)
        if post["created"] > addTzInfo(new_paid_comment):
            new_paid_comment = post["created"].replace(tzinfo=None)
        last_paid_comment = post["created"].replace(tzinfo=None)
        all_votes = ActiveVotes(post["authorperm"], blockchain_instance=hv2)
        for vote in all_votes:
            if vote["voter"] in member_data:
                if member_data[vote["voter"]]["shares"] <= 0:
                    continue
                rshares = vote["rshares"]
                if rshares < 50000000:
                    continue
                rshares = rshares * upvote_multiplier * upvote_multiplier_adjusted

                increment_rshares(member_data, vote, rshares)
                comment_rshares += rshares
    accounts_data[account["name"]]["last_paid_comment"] = last_paid_comment
    accounts_data[account["name"]]["last_paid_post"] = last_paid_post
    print(
        f"hsbi_update_curation_rshares: {post_rshares} new curation rshares for posts"
    )
    print(
        f"hsbi_update_curation_rshares: {comment_rshares} new curation rshares for comments"
    )

    return accounts_data


def run():
    config_file = "config.json"
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        accounts = config_data["accounts"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        hive_blockchain = config_data["hive_blockchain"]

    start_prep_time = time.time()
    db2 = dataset.connect(databaseConnector2)
    db = dataset.connect(databaseConnector)
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)
    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()
    accounts_data = accountStorage.get_data()

    conf_setup = confStorage.get()

    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = ensure_timezone_aware(conf_setup["last_paid_post"])
    last_paid_comment = ensure_timezone_aware(conf_setup["last_paid_comment"])
    upvote_multiplier_adjusted = conf_setup["upvote_multiplier_adjusted"]

    accountTrx = {}
    for account in accounts:
        if account == "steembasicincome":
            accountTrx["sbi"] = AccountTrx(db, "sbi")
        else:
            accountTrx[account] = AccountTrx(db, account)

    print(
        f"hsbi_update_curation_rshares: last_cycle: {formatTimeString(last_cycle)} - {(datetime.now(timezone.utc) - last_cycle).total_seconds() / 60:.2f} min"
    )
    print(
        f"hsbi_update_curation_rshares: last_paid_post: {formatTimeString(last_paid_post)} - last_paid_comment: {formatTimeString(last_paid_comment)}"
    )

    if (datetime.now(timezone.utc) - last_cycle).total_seconds() > 60 * share_cycle_min:
        new_cycle = (
            datetime.now(timezone.utc) - last_cycle
        ).total_seconds() > 60 * share_cycle_min

        print(
            f"hsbi_update_curation_rshares: Update member database, new cycle: {new_cycle}"
        )
        # memberStorage.wipe(True)
        member_accounts = memberStorage.get_all_accounts()

        # print(key_list)
        nodes = NodeList()
        nodes.update_nodes()
        hv = Hive(node=nodes.get_nodes(hive=hive_blockchain))
        hv2 = Hive(node=nodes.get_nodes(hive=hive_blockchain), use_condenser=True)

        member_data = {}
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))

        if True:
            print(
                "hsbi_update_curation_rshares: reward voted steembasicincome post and comments"
            )
            # account = Account("steembasicincome", blockchain_instance=hv)

            if last_paid_post is None:
                last_paid_post = datetime(2018, 8, 9, 3, 36, 48)
            new_paid_post = last_paid_post
            if last_paid_comment is None:
                last_paid_comment = datetime(2018, 8, 9, 3, 36, 48)
            new_paid_comment = last_paid_comment

            for account in accounts:
                accounts_data = update_account(
                    account,
                    new_paid_post,
                    new_paid_comment,
                    conf_setup,
                    accounts_data,
                    hv,
                    accountTrx,
                    member_data,
                    rshares_per_cycle,
                    upvote_multiplier,
                    upvote_multiplier_adjusted,
                    hv2,
                )

        print("hsbi_update_curation_rshares: write member database")
        memberStorage.db = dataset.connect(databaseConnector2)
        member_data_list = []
        for m in member_data:
            member_data_list.append(member_data[m])
        memberStorage.add_batch(member_data_list)
        member_data_list = []
        for acc in accounts_data:
            accountStorage.update(accounts_data[acc])

    print(
        f"hsbi_update_curation_rshares: update curation rshares script run {time.time() - start_prep_time:.2f} s"
    )


if __name__ == "__main__":
    run()
