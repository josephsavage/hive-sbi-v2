import time
from datetime import datetime, timedelta, timezone

from nectar.account import Account
from nectar.blockchain import Blockchain
from nectar.comment import Comment

from hivesbi.member import Member
from hivesbi.settings import get_runtime, make_hive
from hivesbi.storage import ConfigurationDB, KeysDB, MemberDB
from hivesbi.transfer_ops_storage import PostsTrx
from hivesbi.utils import ensure_timezone_aware


def run():
    start_prep_time = time.time()
    rt = get_runtime()
    cfg = rt["cfg"]
    db = rt["db"]
    stor = rt["storages"]
    # Create storages
    memberStorage: MemberDB = stor["member"]
    confStorage: ConfigurationDB = stor["conf"]
    keyStorage: KeysDB = stor["keys"]

    accounts = rt["accounts"]

    conf_setup = confStorage.get()

    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    comment_vote_divider = conf_setup["comment_vote_divider"]
    comment_vote_timeout_h = conf_setup["comment_vote_timeout_h"]
    upvote_delay_correction = 18
    member_accounts = memberStorage.get_all_accounts()

    nobroadcast = False
    # nobroadcast = True

    upvote_counter = {}

    for m in member_accounts:
        upvote_counter[m] = 0

    # print("%d members in list" % len(member_accounts))
    postTrx = PostsTrx(db)

    print("hsbi_upvote_post_comment: Upvote posts/comments")
    start_timestamp = ensure_timezone_aware(datetime(2018, 12, 14, 9, 18, 20))

    keys = []
    for acc in accounts:
        val = keyStorage.get(acc, "posting")
        if val is not None and val.get("key_type") == "posting":
            keys.append(val["wif"].replace("\n", "").replace("\r", ""))

    hv = make_hive(
        cfg,
        keys=keys,
        num_retries=5,
        call_num_retries=3,
        timeout=15,
        nobroadcast=nobroadcast,
    )

    voter_accounts = {}
    for acc in accounts:
        voter_accounts[acc] = Account(acc, blockchain_instance=hv)

    _blockchain = Blockchain(blockchain_instance=hv)
    # print("reading all authorperm")
    rshares_sum = 0
    post_list = postTrx.get_unvoted_post()
    for authorperm in post_list:
        created = ensure_timezone_aware(post_list[authorperm]["created"])
        if (datetime.now(timezone.utc) - created).total_seconds() > 1 * 24 * 60 * 60:
            continue
        if start_timestamp > created:
            continue
        author = post_list[authorperm]["author"]
        if author not in member_accounts:
            continue
        if upvote_counter[author] > 0:
            continue
        if (
            post_list[authorperm]["main_post"] == 0
            and (datetime.now(timezone.utc) - created).total_seconds()
            > comment_vote_timeout_h * 60 * 60
        ):
            postTrx.update_comment_to_old(author, created, True)

        member = Member(memberStorage.get(author))
        if post_list[authorperm]["main_post"] == 0:
            continue
        if member["blacklisted"]:
            continue
        elif member["blacklisted"] is None and (
            member["hivewatchers"] or member["buildawhale"]
        ):
            continue

        if post_list[authorperm]["main_post"] == 1:
            rshares = member["balance_rshares"] / comment_vote_divider
        else:
            rshares = member["balance_rshares"] / (comment_vote_divider**2)
        if post_list[authorperm]["main_post"] == 1 and rshares < minimum_vote_threshold:
            continue
        elif (
            post_list[authorperm]["main_post"] == 0
            and rshares < minimum_vote_threshold * 2
        ):
            continue
        cnt = 0
        c = None
        while c is None and cnt < 5:
            cnt += 1
            try:
                c = Comment(authorperm, blockchain_instance=hv)
            except Exception:
                c = None
                hv.rpc.next()
        if c is None:
            print(f"hsbi_upvote_post_comment: Error getting {authorperm}")
            continue
        _main_post = c.is_main_post()
        already_voted = False
        if c.time_elapsed() >= timedelta(hours=24):
            continue
        voted_after = 300

        for v in c.get_votes():
            if v["voter"] in accounts:
                already_voted = True
                try:
                    if "time" in v:
                        voted_after = (v["time"] - c["created"]).total_seconds()
                    elif "last_update" in v:
                        voted_after = (v["last_update"] - c["created"]).total_seconds()
                    else:
                        voted_after = 300

                except Exception:
                    voted_after = 300
        if already_voted:
            postTrx.update_voted(author, created, already_voted, voted_after)
            continue
        vote_delay_sec = 5 * 60
        if member["upvote_delay"] is not None:
            vote_delay_sec = member["upvote_delay"]
        if c.time_elapsed() < timedelta(
            seconds=(vote_delay_sec - upvote_delay_correction)
        ):
            continue
        if (
            member["last_received_vote"] is not None
            and (
                datetime.now(timezone.utc)
                - ensure_timezone_aware(member["last_received_vote"])
            ).total_seconds()
            / 60
            < 15
        ):
            continue

        if post_list[authorperm]["main_post"] == 0:
            highest_pct = 0
            voter = None
            current_mana = {}
            if rshares > minimum_vote_threshold * 20:
                rshares = int(minimum_vote_threshold * 20)
            for acc in voter_accounts:
                mana = voter_accounts[acc].get_manabar()
                vote_percentage = (
                    rshares
                    / (mana["max_mana"] / 50 * mana["current_mana_pct"] / 100)
                    * 100
                )
                if (
                    highest_pct < mana["current_mana_pct"]
                    and rshares < mana["max_mana"] / 50 * mana["current_mana_pct"] / 100
                    and vote_percentage > 0.01
                ):
                    highest_pct = mana["current_mana_pct"]
                    current_mana = mana
                    voter = acc
            if voter is None:
                voter = "steembasicincome"
                current_mana = voter_accounts[acc].get_manabar()
            vote_percentage = (
                rshares
                / (
                    current_mana["max_mana"]
                    / 50
                    * current_mana["current_mana_pct"]
                    / 100
                )
                * 100
            )

            if nobroadcast and voter is not None:
                print(f"hsbi_upvote_post_comment: {c['authorperm']}")
                print(
                    f"hsbi_upvote_post_comment: Comment Vote {author} from {voter} with {vote_percentage:.2f} %"
                )
            elif voter is not None:
                print(
                    f"hsbi_upvote_post_comment: Comment Upvote {author} from {voter} with {vote_percentage:.2f} %"
                )
                vote_sucessfull = False
                voted_after = 300
                cnt = 0
                vote_time = None
                while not vote_sucessfull and cnt < 5:
                    try:
                        if not Account(voter).has_voted(c):
                            c.upvote(vote_percentage, voter=voter)
                            time.sleep(6)
                            c.refresh()
                            for v in c.get_votes():
                                if voter == v["voter"]:
                                    vote_sucessfull = True
                                    if "time" in v:
                                        vote_time = v["time"]
                                        voted_after = (
                                            v["time"] - c["created"]
                                        ).total_seconds()
                                    else:
                                        vote_time = v["last_update"]
                                        voted_after = (
                                            v["last_update"] - c["created"]
                                        ).total_seconds()
                    except Exception as e:
                        print(e)
                        time.sleep(6)
                        if cnt > 0:
                            c.blockchain.rpc.next()
                        print(
                            f"hsbi_upvote_post_comment: retry to vote {c['authorperm']}"
                        )
                    cnt += 1
                if vote_sucessfull:
                    print(
                        f"hsbi_upvote_post_comment: Vote for {author} at {str(vote_time)} was sucessfully"
                    )
                    memberStorage.update_last_vote(author, vote_time)
                    upvote_counter[author] += 1
                postTrx.update_voted(author, created, vote_sucessfull, voted_after)
        else:
            highest_pct = 0
            voter = None
            current_mana = {}
            pool_rshars = []
            for acc in voter_accounts:
                voter_accounts[acc].refresh()
                mana = voter_accounts[acc].get_manabar()
                vote_percentage = (
                    rshares
                    / (mana["max_mana"] / 50 * mana["current_mana_pct"] / 100)
                    * 100
                )
                if (
                    highest_pct < mana["current_mana_pct"]
                    and rshares < mana["max_mana"] / 50 * mana["current_mana_pct"] / 100
                    and vote_percentage > 0.01
                ):
                    highest_pct = mana["current_mana_pct"]
                    current_mana = mana
                    voter = acc

            if voter is None:
                print(f"hsbi_upvote_post_comment: Could not find voter for {author}")
                current_mana = {}
                pool_rshars = []
                pool_completed = False
                while rshares > 0 and not pool_completed:
                    highest_mana = 0
                    voter = None
                    for acc in voter_accounts:
                        voter_accounts[acc].refresh()
                        mana = voter_accounts[acc].get_manabar()
                        vote_percentage = (
                            rshares
                            / (mana["max_mana"] / 50 * mana["current_mana_pct"] / 100)
                            * 100
                        )
                        if (
                            highest_mana
                            < mana["max_mana"] / 50 * mana["current_mana_pct"] / 100
                            and acc not in pool_rshars
                            and vote_percentage > 0.01
                        ):
                            highest_mana = (
                                mana["max_mana"] / 50 * mana["current_mana_pct"] / 100
                            )
                            current_mana = mana
                            voter = acc
                    if voter is None:
                        pool_completed = True
                        continue
                    pool_rshars.append(voter)
                    vote_percentage = (
                        rshares
                        / (
                            current_mana["max_mana"]
                            / 50
                            * current_mana["current_mana_pct"]
                            / 100
                        )
                        * 100
                    )
                    if vote_percentage > 100:
                        vote_percentage = 100
                    if nobroadcast:
                        print(f"hsbi_upvote_post_comment: {c['authorperm']}")
                        print(
                            f"hsbi_upvote_post_comment: Vote {author} from {voter} with {vote_percentage:.2f} %"
                        )
                    else:
                        print(
                            f"hsbi_upvote_post_comment: Upvote {author} from {voter} with {vote_percentage:.2f} %"
                        )
                        vote_sucessfull = False
                        cnt = 0
                        vote_time = None
                        while not vote_sucessfull and cnt < 5:
                            try:
                                if not Account(voter).has_voted(c):
                                    c.upvote(vote_percentage, voter=voter)
                                    time.sleep(6)
                                    c.refresh()
                                    for v in c.get_votes():
                                        if voter == v["voter"]:
                                            vote_sucessfull = True
                                            if "time" in v:
                                                vote_time = v["time"]
                                            else:
                                                vote_time = v["last_update"]
                            except Exception as e:
                                print(e)
                                time.sleep(6)
                                if cnt > 0:
                                    c.blockchain.rpc.next()
                                print(
                                    f"hsbi_upvote_post_comment: retry to vote {c['authorperm']}"
                                )
                            cnt += 1
                        if vote_sucessfull:
                            print(
                                f"hsbi_upvote_post_comment: Vote for {author} at {str(vote_time)} was sucessfully"
                            )
                            memberStorage.update_last_vote(author, vote_time)
                    rshares_sum += (
                        current_mana["max_mana"]
                        / 50
                        * current_mana["current_mana_pct"]
                        / 100
                    )
                    rshares -= (
                        current_mana["max_mana"]
                        / 50
                        * current_mana["current_mana_pct"]
                        / 100
                    )

            else:
                vote_percentage = (
                    rshares
                    / (
                        current_mana["max_mana"]
                        / 50
                        * current_mana["current_mana_pct"]
                        / 100
                    )
                    * 100
                )
                rshares_sum += (
                    current_mana["max_mana"]
                    / 50
                    * current_mana["current_mana_pct"]
                    / 100
                )
                if nobroadcast:
                    print(f"hsbi_upvote_post_comment: {c['authorperm']}")
                    print(
                        f"hsbi_upvote_post_comment: Vote {author} from {voter} with {vote_percentage:.2f} %"
                    )
                else:
                    print(
                        f"hsbi_upvote_post_comment: Upvote {author} from {voter} with {vote_percentage:.2f} %"
                    )
                    vote_sucessfull = False
                    cnt = 0
                    voted_after = 300
                    vote_time = None
                    while not vote_sucessfull and cnt < 5:
                        try:
                            if not Account(voter).has_voted(c):
                                c.upvote(vote_percentage, voter=voter)
                                time.sleep(6)
                                c.refresh()
                                for v in c.get_votes():
                                    if voter == v["voter"]:
                                        vote_sucessfull = True
                                        if "time" in v:
                                            vote_time = ensure_timezone_aware(v["time"])
                                            voted_after = (
                                                ensure_timezone_aware(v["time"])
                                                - c["created"]
                                            ).total_seconds()
                                        else:
                                            vote_time = ensure_timezone_aware(
                                                v["last_update"]
                                            )
                                            voted_after = (
                                                ensure_timezone_aware(v["last_update"])
                                                - c["created"]
                                            ).total_seconds()
                        except Exception as e:
                            print(e)
                            time.sleep(6)
                            if cnt > 0:
                                c.blockchain.rpc.next()
                            print(
                                f"hsbi_upvote_post_comment: retry to vote {c['authorperm']}"
                            )
                        cnt += 1
                    if vote_sucessfull:
                        print(
                            f"hsbi_upvote_post_comment: Vote for {author} at {str(vote_time)} was sucessfully"
                        )
                        memberStorage.update_last_vote(author, vote_time)
                        upvote_counter[author] += 1
                    postTrx.update_voted(author, created, vote_sucessfull, voted_after)

            print(f"hsbi_upvote_post_comment: rshares_sum {rshares_sum}")
    print(
        f"hsbi_upvote_post_comment: upvote script run {time.time() - start_prep_time:.2f} s"
    )


if __name__ == "__main__":
    run()
