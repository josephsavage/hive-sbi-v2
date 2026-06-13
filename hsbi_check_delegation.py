from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import dataset
from nectar.instance import set_shared_blockchain_instance

from hivesbi.settings import get_runtime, make_hive, Config
from hivesbi.storage import ConfigurationDB, TrxDB
from hivesbi.transfer_ops_storage import TransferTrx
from hivesbi.utils import ensure_timezone_aware


def calculate_shares(delegation_shares, hp_share_ratio):
    """Convert delegated HP into delegation bonus *shares* (HP / sp_share_ratio).

    DEPRECATED (PR #138): delegations no longer grant voting-weight bonus shares.
    On every delegation change or new delegation, hsbi_check_delegation now zeroes
    the delegation trx accrual (clear_delegation_trx) and grants HSBIDAO
    virtual_tokens instead (calculate_virtual_tokens), so this is no longer on the
    production path. Retained intentionally for ad-hoc reporting / historical
    recomputation of what a delegation's share grant would have been.
    """
    return int(delegation_shares / hp_share_ratio)


def calculate_virtual_tokens(delegated_hp):
    return (Decimal(str(delegated_hp)) * Decimal("2")).quantize(
        Decimal("0.001"), rounding=ROUND_HALF_UP
    )


def upsert_virtual_tokens(conn, member_name, virtual_tokens):
    conn.exec_driver_sql(
        """
        INSERT INTO tokenholders (member_name, virtual_tokens)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            virtual_tokens = VALUES(virtual_tokens)
        """,
        (member_name, virtual_tokens),
    )


def clear_delegation_trx(conn, source, account):
    """Zero shares AND vests on the latest valid Delegation trx row.

    hsbi_update_member_db recomputes delegation bonus_shares from shares OR, when
    shares is 0, from vests — so both must be zeroed for the delegation to stop
    generating member accrual and be replaced by virtual_tokens.
    """
    row = conn.exec_driver_sql(
        """
        SELECT `index` FROM trx
        WHERE source = %s AND account = %s
          AND status = 'Valid' AND share_type = 'Delegation'
        ORDER BY `index` DESC
        LIMIT 1
        """,
        (source, account),
    ).fetchone()
    if row is None:
        return
    conn.exec_driver_sql(
        "UPDATE trx SET shares = 0, vests = 0 WHERE `index` = %s AND source = %s",
        (row[0], source),
    )


def apply_active_delegations(conn, source, active_virtual):
    """Clear delegation accrual and set virtual_tokens for each delegator.

    Both writes for a delegator happen in the caller's single transaction, so a
    delegator never ends up with cleared accrual but unset virtual_tokens (or vice
    versa).
    """
    for account, virtual_tokens in active_virtual:
        clear_delegation_trx(conn, source, account)
        upsert_virtual_tokens(conn, account, virtual_tokens)


def clear_virtual_tokens(conn, accounts):
    """Set virtual_tokens to 0 for removed/leased delegators."""
    for account in accounts:
        upsert_virtual_tokens(conn, account, Decimal("0.000"))


def run():
    cfg = Config.load()
    databaseConnector = cfg["databaseConnector"]
    databaseConnector2 = cfg["databaseConnector2"]
    rt = get_runtime()
    
    stor = rt["storages"]
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
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
    last_delegation_check = ensure_timezone_aware(conf_setup["last_delegation_check"])
    previous_delegation_check = last_delegation_check

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
        delegation_share_type = {}
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
                delegation_share_type[d["account"]] = d["share_type"]
                delegation_timestamp[d["account"]] = ensure_timezone_aware(
                    d["timestamp"]
                )
                delegation_shares[d["account"]] = d["shares"]
            elif d["share_type"] == "DelegationLeased":
                delegation[d["account"]] = 0
                delegation_share_type[d["account"]] = d["share_type"]
                delegation_timestamp[d["account"]] = ensure_timezone_aware(
                    d["timestamp"]
                )
                delegation_shares[d["account"]] = d["shares"]
            elif d["share_type"] == "RemovedDelegation":
                delegation[d["account"]] = 0
                delegation_share_type[d["account"]] = d["share_type"]
                delegation_timestamp[d["account"]] = ensure_timezone_aware(
                    d["timestamp"]
                )
                delegation_shares[d["account"]] = 0

        delegation_leased = {}
        delegation_shares = {}
        # Collected token changes, applied together in one batched transaction.
        active_virtual = []  # [(account, virtual_tokens)] -> clear accrual + set virtual
        zero_virtual = []    # [account] -> set virtual_tokens = 0 (removed/leased)
        print("hsbi_check_delegation: update delegation")
        delegation_account = delegation
        for acc in delegation_account:
            if delegation_account[acc] == 0:
                continue
            if (
                previous_delegation_check is not None
                and delegation_timestamp[acc] <= previous_delegation_check
            ):
                continue
            if (
                last_delegation_check is not None
                and last_delegation_check < delegation_timestamp[acc]
            ):
                last_delegation_check = delegation_timestamp[acc]
            elif last_delegation_check is None:
                last_delegation_check = delegation_timestamp[acc]
            print(f"hsbi_check_delegation: {acc}")
            leased = transferStorage.find(acc, account)
            if len(leased) == 0:
                delegation_shares[acc] = delegation_account[acc]
                virtual_tokens = calculate_virtual_tokens(delegation_account[acc])
                active_virtual.append((acc, virtual_tokens))
                print(
                    f"hsbi_check_delegation: will set {acc} virtual_tokens to {virtual_tokens}"
                )
                continue
            delegation_leased[acc] = delegation_account[acc]
            trxStorage.update_delegation_state(
                account, acc, "Delegation", "DelegationLeased"
            )
            zero_virtual.append(acc)
            print(f"hsbi_check_delegation: set delegation from {acc} to leased")

        for acc, share_type in delegation_share_type.items():
            if share_type not in ["RemovedDelegation", "DelegationLeased"]:
                continue
            if (
                previous_delegation_check is not None
                and delegation_timestamp[acc] <= previous_delegation_check
            ):
                continue
            zero_virtual.append(acc)
            if (
                last_delegation_check is not None
                and last_delegation_check < delegation_timestamp[acc]
            ):
                last_delegation_check = delegation_timestamp[acc]
            elif last_delegation_check is None:
                last_delegation_check = delegation_timestamp[acc]
            print(f"hsbi_check_delegation: will clear virtual_tokens for {acc}")

        # Apply all delegation token changes atomically in one batched transaction.
        if active_virtual or zero_virtual:
            with db2.engine.begin() as conn:
                apply_active_delegations(conn, account, active_virtual)
                clear_virtual_tokens(conn, zero_virtual)

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
