from hivesbi.settings import get_runtime, make_hive


def run():
    rt = get_runtime()
    cfg = rt["cfg"]
    # Create storages
    stor = rt["storages"]
    memberStorage = stor["member"]

    hv = make_hive(cfg)

    print("hsbi_check_member_db: check member database")
    # memberStorage.wipe(True)
    member_accounts = memberStorage.get_all_accounts()

    missing_accounts = []
    member_data = {}
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


if __name__ == "__main__":
    run()
