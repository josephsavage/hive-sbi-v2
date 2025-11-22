import json
import time
from datetime import datetime, timedelta, timezone
from time import sleep

import dataset
from nectar import Hive
from nectar.account import Account
from nectar.nodelist import NodeList
from nectar.utils import (
    formatTimeString,
)

from hivesbi.member import Member
from hivesbi.settings import get_runtime, make_hive, Config
from hivesbi.storage import (
    AccountsDB,
    ConfigurationDB,
    KeysDB,
    MemberDB,
    TransferMemoDB,
    TrxDB,
)
from hivesbi.transfer_ops_storage import AccountTrx
from hivesbi.utils import ensure_timezone_aware


def memo_welcome(transferMemos, memo_transfer_acc, sponsor, HIVE_symbol="HIVE"):
    if "welcome" not in transferMemos:
        return

    if transferMemos["welcome"]["enabled"] == 0:
        return
    if memo_transfer_acc is None:
        return
    try:
        memo_text = transferMemos["welcome"]["memo"]
        memo_transfer_acc.transfer(sponsor, 0.001, HIVE_symbol, memo=memo_text)
        sleep(4)
    except Exception:
        print(f"hsbi_update_member_db: Could not send 0.001 {HIVE_symbol} to {sponsor}")


def memo_sponsoring(transferMemos, memo_transfer_acc, s, sponsor, HIVE_symbol="HIVE"):
    if "sponsoring" not in transferMemos:
        return
    if transferMemos["sponsoring"]["enabled"] == 0:
        return
    if memo_transfer_acc is None:
        return
    try:
        if "%s" in transferMemos["sponsoring"]["memo"]:
            memo_text = transferMemos["sponsoring"]["memo"] % sponsor
        else:
            memo_text = transferMemos["sponsoring"]["memo"]
        memo_transfer_acc.transfer(s, 0.001, HIVE_symbol, memo=memo_text)
        sleep(4)
    except Exception:
        print(f"hsbi_update_member_db: Could not send 0.001 {HIVE_symbol} to {s}")


def memo_update_shares(
    transferMemos, memo_transfer_acc, sponsor, shares, HIVE_symbol="HIVE"
):
    if "update_shares" not in transferMemos:
        return
    if transferMemos["update_shares"]["enabled"] == 0:
        return
    if memo_transfer_acc is None:
        return
    try:
        if "%d" in transferMemos["update_shares"]["memo"]:
            memo_text = transferMemos["update_shares"]["memo"] % shares
        else:
            memo_text = transferMemos["update_shares"]["memo"]
        memo_transfer_acc.transfer(sponsor, 0.001, HIVE_symbol, memo=memo_text)
        sleep(4)
    except Exception:
        print(f"hsbi_update_member_db: Could not send 0.001 {HIVE_symbol} to {sponsor}")


def memo_sponsoring_update_shares(
    transferMemos, memo_transfer_acc, s, sponsor, shares, HIVE_symbol="HIVE"
):
    if "sponsoring_update_shares" not in transferMemos:
        return
    if transferMemos["sponsoring_update_shares"]["enabled"] == 0:
        return
    if memo_transfer_acc is None:
        return
    try:
        if (
            "%s" in transferMemos["sponsoring_update_shares"]["memo"]
            and "%d" in transferMemos["sponsoring_update_shares"]["memo"]
        ):
            if transferMemos["sponsoring_update_shares"]["memo"].find(
                "%s"
            ) < transferMemos["sponsoring_update_shares"]["memo"].find("%d"):
                memo_text = transferMemos["sponsoring_update_shares"]["memo"] % (
                    sponsor,
                    shares,
                )
            else:
                memo_text = transferMemos["sponsoring_update_shares"]["memo"] % (
                    shares,
                    sponsor,
                )
        elif "%s" in transferMemos["sponsoring_update_shares"]["memo"]:
            memo_text = transferMemos["sponsoring_update_shares"]["memo"] % sponsor
        else:
            memo_text = transferMemos["sponsoring_update_shares"]["memo"]
        memo_transfer_acc.transfer(s, 0.001, HIVE_symbol, memo=memo_text)
        sleep(4)
    except Exception:
        print("Could not sent 0.001 %s to %s" % (HIVE_symbol, s))


def run():
    cfg = Config.load()
    databaseConnector = cfg["databaseConnector"]
    databaseConnector2 = cfg["databaseConnector2"]
    mgnt_shares = cfg["mgnt_shares"]
    hive_blockchain = cfg["hive_blockchain"]
    
    rt = get_runtime()
    stor = rt["storages"]
    start_prep_time = time.time()
    db2 = dataset.connect(databaseConnector2)
    db = dataset.connect(databaseConnector)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    keyStorage = KeysDB(db2)
    memberStorage = MemberDB(db2)
    # accountStorage = MemberHistDB(db)
    confStorage = ConfigurationDB(db2)
    confStorage: ConfigurationDB = stor["conf"]

    transferMemosStorage = TransferMemoDB(db2)
    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()

    conf_setup = confStorage.get()

    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])
    share_cycle_min = conf_setup["share_cycle_min"]
    hp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    del_rshares_per_cycle = conf_setup["del_rshares_per_cycle"]
    last_paid_post = ensure_timezone_aware(conf_setup["last_paid_post"])
    last_paid_comment = ensure_timezone_aware(conf_setup["last_paid_comment"])
    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    comment_vote_divider = conf_setup["comment_vote_divider"]
    
    if db2 is not None:
        with db2.engine.begin() as conn:
            result = conn.exec_driver_sql(
                "SELECT MAX(mana_pct) AS max_mana_pct FROM accounts"
            ).fetchone()
            max_mana_pct = result.max_mana_pct if result and result.max_mana_pct else 0
            print("hsbi_update_member_db fetching max VP level:", max_mana_pct)

    
    mana_pct_target = conf_setup.get("mana_pct_target", 0)
    mana_threshold = conf_setup.get("mana_threshold", 0)
    max_mana_threshold = mana_threshold * mana_pct_target
                
    accountTrx = {}
    for account in accounts:
        if account == "steembasicincome":
            accountTrx["sbi"] = AccountTrx(db, "sbi")
        else:
            accountTrx[account] = AccountTrx(db, account)

    print(
        f"hsbi_update_member_db: last_cycle: {formatTimeString(last_cycle)} - {(datetime.now(timezone.utc) - last_cycle).total_seconds() / 60:.2f} min"
    )
    print(
        f"hsbi_update_member_db: last_paid_post: {formatTimeString(last_paid_post)} - last_paid_comment: {formatTimeString(last_paid_comment)}"
    )
    
    if last_cycle is None:
        last_cycle = datetime.now(timezone.utc) - timedelta(seconds=60 * 145)
        confStorage.update({"last_cycle": last_cycle})
    elif (
        (max_mana_pct is not None and max_mana_pct > max_mana_threshold)
        or (
            last_cycle is not None
            and (datetime.now(timezone.utc) - last_cycle).total_seconds() > 60 * share_cycle_min
        )
    ):
        # your logic here
        new_cycle = (max_mana_pct > max_mana_threshold)

        # memberStorage.wipe(True)
        member_accounts = memberStorage.get_all_accounts()
        data = trxStorage.get_all_data()

        data = sorted(
            data,
            key=lambda x: (
                datetime.now(timezone.utc) - ensure_timezone_aware(x["timestamp"])
            ).total_seconds(),
            reverse=True,
        )

        # Update current node list from @fullnodeupdate
        keys_list = []
        key = keyStorage.get("steembasicincome", "memo")
        if key is not None:
            keys_list.append(key["wif"].replace("\n", "").replace("\r", ""))

        memo_transfer_acc = accountStorage.get_transfer_memo_sender()
        if len(memo_transfer_acc) > 0:
            memo_transfer_acc = memo_transfer_acc[0]
        key = keyStorage.get(memo_transfer_acc, "active")
        if key is not None and key["key_type"] == "active":
            keys_list.append(key["wif"].replace("\n", "").replace("\r", ""))

        transferMemos = {}
        for db_entry in transferMemosStorage.get_all_data():
            transferMemos[db_entry["memo_type"]] = {
                "enabled": db_entry["enabled"],
                "memo": db_entry["memo"],
            }

        # print(key_list)
        nodes = NodeList()
        nodes.update_nodes()
        hv = Hive(keys=keys_list, node=nodes.get_nodes(hive=hive_blockchain))

        if memo_transfer_acc is not None:
            try:
                memo_transfer_acc = Account(memo_transfer_acc, blockchain_instance=hv)
            except Exception:
                print(
                    f"hsbi_update_member_db: {memo_transfer_acc} is not a valid hive account! Will be able to send transfer memos..."
                )

        member_data = {}
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))

        mngt_shares_assigned = False
        mngt_shares = 0
        delegation = {}
        delegation_timestamp = {}
        # clear shares
        for m in member_data:
            member_data[m]["shares"] = 0
            member_data[m]["bonus_shares"] = 0
            delegation[m] = 0
            delegation_timestamp[m] = None
            member_data[m].reset_share_age_list()

        shares_sum = 0
        latest_share = trxStorage.get_lastest_share_type("Mgmt")
        mngt_shares_sum = (latest_share["index"] + 1) / len(mgnt_shares) * 100
        print(f"hsbi_update_member_db: mngt_shares sum {mngt_shares_sum}")
        latest_data_timestamp = None

        for op in data:
            if op["status"] == "Valid":
                share_type = op["share_type"]
                if latest_data_timestamp is None:
                    latest_data_timestamp = formatTimeString(op["timestamp"])
                elif latest_data_timestamp < formatTimeString(op["timestamp"]):
                    latest_data_timestamp = formatTimeString(op["timestamp"])
                if share_type in ["DelegationLeased"]:
                    continue
                if isinstance(op["timestamp"], str):
                    timestamp = formatTimeString(op["timestamp"])
                else:
                    timestamp = op["timestamp"]
                if share_type.lower() in ["sharetransfer"]:
                    if (
                        op["shares"] > 0
                        and op["sponsor"] in member_data
                        and op["account"] in member_data
                    ):
                        if op["shares"] > member_data[op["account"]]["shares"]:
                            continue
                        member_data[op["account"]]["shares"] -= op["shares"]
                        member_data[op["sponsor"]]["shares"] += op["shares"]

                        member_data[op["sponsor"]]["latest_enrollment"] = timestamp
                        member_data[op["sponsor"]].append_share_age(
                            timestamp, op["shares"]
                        )
                elif share_type.lower() in ["delegation"]:
                    if op["shares"] > 0 and op["sponsor"] in member_data:
                        # print("del. bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
                        delegation[op["sponsor"]] = op["shares"]
                    elif op["vests"] > 0 and op["sponsor"] in member_data:
                        sp = hv.vests_to_hp(float(op["vests"]))
                        delegation[op["sponsor"]] = int(sp / hp_share_ratio)
                    # memo_hp_delegation(transferMemos, memo_transfer_acc, op["sponsor"], delegation[op["sponsor"]], hp_share_ratio)
                    delegation_timestamp[op["sponsor"]] = timestamp
                elif share_type.lower() in ["removeddelegation"]:
                    delegation[op["sponsor"]] = 0
                    delegation_timestamp[op["sponsor"]] = None

                elif share_type.lower() in ["mgmttransfer"]:
                    if (
                        op["shares"] > 0
                        and op["sponsor"] in member_data
                        and op["account"] in member_data
                    ):
                        if op["shares"] > member_data[op["account"]]["bonus_shares"]:
                            continue
                        member_data[op["account"]]["bonus_shares"] -= op["shares"]
                        member_data[op["sponsor"]]["bonus_shares"] += op["shares"]

                        member_data[op["sponsor"]]["latest_enrollment"] = timestamp
                        member_data[op["sponsor"]].append_share_age(
                            timestamp, op["shares"]
                        )

                elif share_type.lower() in ["mgmt"]:
                    if op["shares"] > 0 and op["sponsor"] in member_data:
                        member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
                        member_data[op["sponsor"]].append_share_age(
                            timestamp, op["shares"]
                        )
                        mngt_shares += op["shares"]
                    else:
                        member = Member(op["sponsor"], op["shares"], timestamp)
                        member.append_share_age(timestamp, op["shares"])
                        member_data[op["sponsor"]] = member
                        print(
                            f"hsbi_update_member_db: mngt bonus_shares: {op['sponsor']} - {op['shares']}"
                        )
                else:
                    sponsor = op["sponsor"]
                    try:
                        sponsee = json.loads(op["sponsee"].replace('""', '"'))
                    except Exception:
                        continue
                    shares = op["shares"]
                    if isinstance(op["timestamp"], str):
                        timestamp = formatTimeString(op["timestamp"])
                    else:
                        timestamp = op["timestamp"]

                    if shares == 0:
                        continue
                    shares_sum += shares
                    for s in sponsee:
                        shares_sum += sponsee[s]
                    # if (shares_sum - mngt_shares_sum) >= 100:

                    if sponsor not in member_data:
                        # Build and send transfer with memo to welcome new member
                        memo_welcome(
                            transferMemos,
                            memo_transfer_acc,
                            sponsor,
                            HIVE_symbol=hv.hive_symbol,
                        )

                        member = Member(sponsor, shares, timestamp)
                        member.append_share_age(timestamp, shares)
                        member_data[sponsor] = member
                        member_data[sponsor]["balance_rshares"] = (
                            minimum_vote_threshold * comment_vote_divider
                        )
                    else:
                        member_data[sponsor]["latest_enrollment"] = timestamp
                        member_data[sponsor]["shares"] += shares

                        # Build and send transfer with memo about new shares
                        memo_update_shares(
                            transferMemos,
                            memo_transfer_acc,
                            sponsor,
                            member_data[sponsor]["shares"],
                            HIVE_symbol=hv.hive_symbol,
                        )
                        member_data[sponsor].append_share_age(timestamp, shares)

                    if len(sponsee) == 0:
                        continue
                    for s in sponsee:
                        shares = sponsee[s]
                        if s not in member_data:
                            # Build and send transfer with memo to welcome new sponsered member
                            memo_sponsoring(
                                transferMemos,
                                memo_transfer_acc,
                                s,
                                sponsor,
                                HIVE_symbol=hv.hive_symbol,
                            )

                            member = Member(s, shares, timestamp)
                            member.append_share_age(timestamp, shares)
                            member_data[s] = member
                            member_data[s]["balance_rshares"] = (
                                minimum_vote_threshold * comment_vote_divider
                            )
                        else:
                            member_data[s]["latest_enrollment"] = timestamp
                            member_data[s]["shares"] += shares
                            # Build and send transfer with memo about new sponsored shares
                            memo_sponsoring_update_shares(
                                transferMemos,
                                memo_transfer_acc,
                                s,
                                sponsor,
                                member_data[s]["shares"],
                                HIVE_symbol=hv.hive_symbol,
                            )
                            member_data[s].append_share_age(timestamp, shares)
                            

        print(
            "mngt_shares: %d, shares_sum %d - (mngt_shares * 20): %d - shares_sum - 100: %d"
            % (mngt_shares, shares_sum, (mngt_shares * 20), shares_sum - 100)
        )
        if (mngt_shares * 20) < shares_sum - 100 and not mngt_shares_assigned:
            mngt_shares_assigned = True
            mngt_shares_sum += 100
            print("hsbi_update_member_db: add mngt shares")
            latest_share = trxStorage.get_lastest_share_type("Mgmt")
            if latest_share is not None:
                start_index = latest_share["index"] + 1
            else:
                start_index = 0
            for account in mgnt_shares:
                shares = mgnt_shares[account]
                mgmt_data = {
                    "index": start_index,
                    "source": "mgmt",
                    "memo": "",
                    "account": account,
                    "sponsor": account,
                    "sponsee": {},
                    "shares": shares,
                    "vests": float(0),
                    "timestamp": formatTimeString(timestamp),
                    "status": "Valid",
                    "share_type": "Mgmt",
                }
                start_index += 1
                print(f"hsbi_update_member_db: {mgmt_data}")
                trxStorage.add(mgmt_data)

        # add bonus_shares from active delegation
        for m in member_data:
            if m in delegation:
                member_data[m]["bonus_shares"] += delegation[m]
            if (
                m in delegation_timestamp
                and delegation_timestamp[m] is not None
                and m in delegation
            ):
                member_data[m].append_share_age(delegation_timestamp[m], delegation[m])

        print("hsbi_update_member_db: update share age")

        empty_shares = []
        latest_enrollment = None
        for m in member_data:
            if member_data[m]["shares"] <= 0:
                empty_shares.append(m)
                member_data[m]["total_share_days"] = 0
                member_data[m]["avg_share_age"] = 0
                continue
            member_data[m].calc_share_age()
            if latest_enrollment is None:
                latest_enrollment = member_data[m]["latest_enrollment"]
            elif latest_enrollment < member_data[m]["latest_enrollment"]:
                latest_enrollment = member_data[m]["latest_enrollment"]

        print(
            f"hsbi_update_member_db: latest data timestamp: {str(latest_data_timestamp)} - latest member enrollment {str(latest_enrollment)}"
        )

        if new_cycle:
            try:
                for m in member_data:
                    current_cycle = last_cycle + timedelta(seconds=60 * share_cycle_min)
                    if member_data[m]["shares"] <= 0:
                        continue
                    if "first_cycle_at" not in member_data[m]:
                        member_data[m]["first_cycle_at"] = ensure_timezone_aware(
                            current_cycle
                        )
                    elif ensure_timezone_aware(
                        member_data[m]["first_cycle_at"]
                    ) < ensure_timezone_aware(datetime(2000, 1, 1, 0, 0, 0)):
                        member_data[m]["first_cycle_at"] = ensure_timezone_aware(
                            current_cycle
                        )
                    member_data[m]["balance_rshares"] += (
                        member_data[m]["shares"] * rshares_per_cycle
                    ) + (member_data[m]["bonus_shares"] * del_rshares_per_cycle)
                    member_data[m]["earned_rshares"] += (
                        member_data[m]["shares"] * rshares_per_cycle
                    ) + (member_data[m]["bonus_shares"] * del_rshares_per_cycle)
                    member_data[m]["subscribed_rshares"] += (
                        member_data[m]["shares"] * rshares_per_cycle
                    )
                    member_data[m]["delegation_rshares"] += (
                        member_data[m]["bonus_shares"] * del_rshares_per_cycle
                    )                    
            except Exception as e:
                print(f"Error calling stored procedure: {e}")

        print("hsbi_update_member_db: write member database")
        memberStorage.db = dataset.connect(databaseConnector2)
        member_data_list = []
        member_data_json = []
        for m in member_data:
            member_data_list.append(member_data[m])
            member_json = member_data[m].copy()
            if (
                "last_comment" in member_json
                and member_json["last_comment"] is not None
            ):
                member_json["last_comment"] = str(member_json["last_comment"])
            if "last_post" in member_json and member_json["last_post"] is not None:
                member_json["last_post"] = str(member_json["last_post"])
            if (
                "original_enrollment" in member_json
                and member_json["original_enrollment"] is not None
            ):
                member_json["original_enrollment"] = str(
                    member_json["original_enrollment"]
                )
            if (
                "latest_enrollment" in member_json
                and member_json["latest_enrollment"] is not None
            ):
                member_json["latest_enrollment"] = str(member_json["latest_enrollment"])
            if "updated_at" in member_json and member_json["updated_at"] is not None:
                member_json["updated_at"] = str(member_json["updated_at"])
            if (
                "first_cycle_at" in member_json
                and member_json["first_cycle_at"] is not None
            ):
                member_json["first_cycle_at"] = str(member_json["first_cycle_at"])
            if (
                "last_received_vote" in member_json
                and member_json["last_received_vote"] is not None
            ):
                member_json["last_received_vote"] = str(
                    member_json["last_received_vote"]
                )
            member_data_json.append(member_json)
        memberStorage.add_batch(member_data_list)
        member_data_list = []
        with open("/var/www/html/data.json", "w") as outfile:
            json.dump(member_data_json, outfile)
        member_data_json = []


        # Statistics
        shares = 0
        bonus_shares = 0

        delegation_shares = 0
        for m in member_data:
            shares += member_data[m]["shares"]
            bonus_shares += member_data[m]["bonus_shares"]
            if m in delegation:
                delegation_shares += delegation[m]
        print(f"hsbi_update_member_db: shares: {shares}")
        print(f"hsbi_update_member_db: delegation bonus shares: {delegation_shares}")
        print(f"hsbi_update_member_db: Mngt bonus shares {mngt_shares}")
        
        print(f"hsbi_update_member_db: update last_cycle to {str(datetime.now(timezone.utc))}")
        confStorage.update({"last_cycle": datetime.now(timezone.utc)})



    print(
        f"hsbi_update_member_db: update member script run {time.time() - start_prep_time:.2f} s"
    )
if __name__ == "__main__":
    run()
