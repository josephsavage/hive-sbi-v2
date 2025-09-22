import json

from nectar.utils import formatTimeString

from hivesbi.member import Member
from hivesbi.settings import get_runtime


def run():
    rt = get_runtime()
    cfg = rt["cfg"]
    print(f"hsbi_build_member_db: {dict(cfg.items())}")
    stor = rt["storages"]
    # Create storages
    trxStorage = stor["trx"]
    memberStorage = stor["member"]

    if not trxStorage.exists_table():
        trxStorage.create_table()

    if not memberStorage.exists_table():
        memberStorage.create_table()

    # Update current node list from @fullnodeupdate
    print("hsbi_build_member_db: build member database")
    # memberStorage.wipe(True)
    accs = memberStorage.get_all_accounts()
    for a in accs:
        memberStorage.delete(a)
    data = trxStorage.get_all_data()
    share_type = {}
    member_data = {}
    for op in data:
        if op["status"] == "Valid":
            share_type = op["share_type"]
            if share_type in [
                "RemovedDelegation",
                "Delegation",
                "DelegationLeased",
                "Mgmt",
                "MgmtTransfer",
            ]:
                continue
            sponsor = op["sponsor"]
            sponsee = json.loads(op["sponsee"])
            shares = op["shares"]
            if isinstance(op["timestamp"], str):
                timestamp = formatTimeString(op["timestamp"])
            else:
                timestamp = op["timestamp"]
            if shares == 0:
                continue
            if sponsor not in member_data:
                member = Member(sponsor, shares, timestamp)
                member.append_share_age(timestamp, shares)
                member_data[sponsor] = member
            else:
                member_data[sponsor]["latest_enrollment"] = timestamp
                member_data[sponsor]["shares"] += shares
                member_data[sponsor].append_share_age(timestamp, shares)
            if len(sponsee) == 0:
                continue
            for s in sponsee:
                shares = sponsee[s]
                if s not in member_data:
                    member = Member(s, shares, timestamp)
                    member.append_share_age(timestamp, shares)
                    member_data[s] = member
                else:
                    member_data[s]["latest_enrollment"] = timestamp
                    member_data[s]["shares"] += shares
                    member_data[s].append_share_age(timestamp, shares)

    empty_shares = []
    for m in member_data:
        if member_data[m]["shares"] <= 0:
            empty_shares.append(m)

    for del_acc in empty_shares:
        del member_data[del_acc]

    shares = 0
    bonus_shares = 0
    for m in member_data:
        member_data[m].calc_share_age()
        shares += member_data[m]["shares"]
        bonus_shares += member_data[m]["bonus_shares"]
    print(f"hsbi_build_member_db: shares: {shares}")
    print(f"hsbi_build_member_db: bonus shares: {bonus_shares}")
    print(f"hsbi_build_member_db: total shares: {shares + bonus_shares}")

    member_list = []
    for m in member_data:
        member_list.append(member_data[m])
    memberStorage.add_batch(member_list)


if __name__ == "__main__":
    run()
