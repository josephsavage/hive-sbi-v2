from datetime import datetime, timezone

import dataset
from nectar.instance import set_shared_blockchain_instance

from hivesbi.settings import get_runtime, make_hive, Config
from hivesbi.storage import ConfigurationDB, TrxDB
from hivesbi.transfer_ops_storage import TransferTrx
from hivesbi.utils import ensure_timezone_aware


def calculate_shares(delegation_shares, hp_share_ratio):
    return int(delegation_shares / hp_share_ratio)


def run():
    cfg = Config.load()
    databaseConnector = cfg["databaseConnector"]
    databaseConnector2 = cfg["databaseConnector2"]
    rt = get_runtime()
    
    stor = rt["storages"]
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    confStorage = ConfigurationDB(db2)
    confStorage: ConfigurationDB = stor["conf"]
    conf_setup = confStorage.get()

    if db2 is not None:
        with db2.engine.begin() as conn:
            result = conn.exec_driver_sql(
                "SELECT MAX(mana_pct) AS max_mana_pct FROM accounts"
            ).fetchone()
            max_mana_pct = result.max_mana_pct if result and result.max_mana_pct else 0
            print("hsbi_check_delegation fetching max VP level:", max_mana_pct)

    
    mana_pct_target = conf_setup.get("mana_pct_target", 0)
    mana_threshold = conf_setup.get("mana_threshold", 0)
    max_mana_threshold = mana_threshold * mana_pct_target
    last_cycle = ensure_timezone_aware(conf_setup["last_cycle"])

    share_cycle_min = conf_setup["share_cycle_min"]
    hp_share_ratio = conf_setup["sp_share_ratio"]
    last_delegation_check = ensure_timezone_aware(conf_setup["last_delegation_check"])

    if (
        (max_mana_pct is not None and max_mana_pct > max_mana_threshold)
        or (
            last_cycle is not None
            and (datetime.now(timezone.utc) - last_cycle).total_seconds() > 60 * share_cycle_min
        )
    ):
        # your logic here
        hv = make_hive(cfg)
        set_shared_blockchain_instance(hv)

        transferStorage = TransferTrx(db)
        trxStorage = TrxDB(db2)

        delegation = {}
        delegation_shares = {}
        sum_hp = 0
        sum_hp_leased = 0
        sum_hp_shares = 0
        delegation_timestamp = {}
        account = "steembasicincome"
        print("hsbi_check_delegation: load delegation")
        delegation_list = []
        for d in trxStorage.get_share_type(share_type="Delegation"):
            if d["share_type"] == "Delegation":
                delegation_list.append(d)
        for d in trxStorage.get_share_type(share_type="DelegationLeased"):
            if d["share_type"] == "DelegationLeased":
                delegation_list.append(d)
        for d in trxStorage.get_share_type(share_type="RemovedDelegation"):
            if d["share_type"] == "RemovedDelegation":
                delegation_list.append(d)

        sorted_delegation_list = sorted(
            delegation_list,
            key=lambda x: (
                datetime.now(timezone.utc) - ensure_timezone_aware(x["timestamp"])
            ).total_seconds(),
            reverse=True,
        )

        for d in sorted_delegation_list:
            if d["share_type"] == "Delegation":
                delegation[d["account"]] = hv.vests_to_hp(float(d["vests"]))
                delegation_timestamp[d["account"]] = ensure_timezone_aware(
                    d["timestamp"]
                )
                delegation_shares[d["account"]] = d["shares"]
            elif d["share_type"] == "DelegationLeased":
                delegation[d["account"]] = 0
                delegation_timestamp[d["account"]] = ensure_timezone_aware(
                    d["timestamp"]
                )
                delegation_shares[d["account"]] = d["shares"]
            elif d["share_type"] == "RemovedDelegation":
                delegation[d["account"]] = 0
                delegation_timestamp[d["account"]] = ensure_timezone_aware(
                    d["timestamp"]
                )
                delegation_shares[d["account"]] = 0

        delegation_leased = {}
        delegation_shares = {}
        print("hsbi_check_delegation: update delegation")
        delegation_account = delegation
        for acc in delegation_account:
            if delegation_account[acc] == 0:
                continue
            if (
                last_delegation_check is not None
                and delegation_timestamp[acc] <= last_delegation_check
            ):
                continue
            if (
                last_delegation_check is not None
                and last_delegation_check < delegation_timestamp[acc]
            ):
                last_delegation_check = delegation_timestamp[acc]
            elif last_delegation_check is None:
                last_delegation_check = delegation_timestamp[acc]
            # if acc in delegation_shares and delegation_shares[acc] > 0:
            #    continue
            print(f"hsbi_check_delegation: {acc}")
            leased = transferStorage.find(acc, account)
            if len(leased) == 0:
                delegation_shares[acc] = delegation_account[acc]
                shares = calculate_shares(delegation_account[acc], hp_share_ratio)
                trxStorage.update_delegation_shares(account, acc, shares)
                continue
            delegation_leased[acc] = delegation_account[acc]
            trxStorage.update_delegation_state(
                account, acc, "Delegation", "DelegationLeased"
            )
            print(f"hsbi_check_delegation: set delegation from {acc} to leased")

        dd = delegation
        for d in dd:
            sum_hp += dd[d]
        dd = delegation_leased
        for d in dd:
            sum_hp_leased += dd[d]
        dd = delegation_shares
        for d in dd:
            sum_hp_shares += dd[d]
        print(
            f"hsbi_check_delegation: {account}: sum {sum_hp:.6f} HP - shares {sum_hp_shares:.6f} HP - leased {sum_hp_leased:.6f} HP"
        )

        confStorage.update({"last_delegation_check": last_delegation_check})


if __name__ == "__main__":
    run()
