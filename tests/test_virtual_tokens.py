"""Tests for delegation virtual_tokens, write-ahead issuance, and chain-confirm
reconciliation.

The pure-logic tests run anywhere. The DB-level tests connect to the local docker
MariaDB (the `sbi` database via config.json `databaseConnector2`) and run inside a
transaction that is rolled back, so they leave no residue. They skip automatically
when the database is unreachable (e.g. running on the host without the container).
"""

import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from hsbi_check_delegation import (
    apply_active_delegations,
    calculate_virtual_tokens,
    clear_delegation_trx,
)
from hsbi_token_snapshot import (
    _parse_engine_issue,
    calculate_management_issue_amount,
    insert_pending_issuance,
    reconcile_issuances,
    select_issuable_balances,
)

# --- sentinel names so DB tests never collide with real members ---------------
T_DEL = "zz_vt_delegator"
T_DEL2 = "zz_vt_delegator2"
T_PIK = "zz_vt_pik"
T_HOLDER = "zz_vt_holder"
T_GUARD = "zz_vt_guard"
T_MGMT = "josephsavage"
T_TRX_INDEX = 990000001
T_SOURCE = "steembasicincome"


# --- try to open the sbi database; DB tests skip if unavailable ---------------
_DB_ENGINE = None
_DB_SKIP_REASON = ""
try:
    import dataset
    from hivesbi.settings import Config

    _cfg = Config.load()
    _DB_ENGINE = dataset.connect(_cfg["databaseConnector2"]).engine
    _DB_ENGINE.connect().close()  # probe
except Exception as exc:  # pragma: no cover - environment dependent
    _DB_SKIP_REASON = f"sbi database unavailable: {exc}"
    _DB_ENGINE = None


class PureLogicTests(unittest.TestCase):
    def test_virtual_tokens_are_decimal_hp_times_two(self):
        # No integer rounding: 0.001 HP delegated -> 0.002 virtual tokens.
        self.assertEqual(calculate_virtual_tokens("0.001"), Decimal("0.002"))
        self.assertEqual(calculate_virtual_tokens("123.4567"), Decimal("246.913"))

    def test_management_formula_floors_to_three_decimals(self):
        # 0.10 * 1000 / 0.90 = 111.111... -> floored to 111.111
        self.assertEqual(
            calculate_management_issue_amount(Decimal("1000.000"), Decimal("0")),
            Decimal("111.111"),
        )
        # cap already met -> 0
        self.assertEqual(
            calculate_management_issue_amount(Decimal("1000.000"), Decimal("100.000")),
            Decimal("0.000"),
        )

    def test_management_converges_after_one_issue(self):
        outstanding = Decimal("1000.000")
        first = calculate_management_issue_amount(outstanding, Decimal("0"))
        # next cycle: issued tokens are now real supply, and counted as issued
        second = calculate_management_issue_amount(outstanding + first, first)
        self.assertEqual(second, Decimal("0.000"))

    def test_parse_engine_issue_custom_json(self):
        row = {
            "trx_id": "abc123",
            "op": [
                "custom_json",
                {
                    "json": json.dumps(
                        {
                            "contractName": "tokens",
                            "contractAction": "issue",
                            "contractPayload": {
                                "symbol": "HSBIDAO",
                                "to": "josephsavage",
                                "quantity": "12.3456",
                            },
                        }
                    )
                },
            ],
        }
        self.assertEqual(
            _parse_engine_issue(row),
            {"trx_id": "abc123", "recipient": "josephsavage", "units": Decimal("12.345")},
        )

    def test_parse_engine_issue_ignores_non_issue(self):
        row = {
            "trx_id": "x",
            "op": ["custom_json", {"json": json.dumps({"contractName": "market"})}],
        }
        self.assertIsNone(_parse_engine_issue(row))


@unittest.skipUnless(_DB_ENGINE is not None, _DB_SKIP_REASON)
class DBTestCase(unittest.TestCase):
    """Base class: each test runs in a transaction that is rolled back."""

    def setUp(self):
        self.conn = _DB_ENGINE.connect()
        self.trans = self.conn.begin()

    def tearDown(self):
        self.trans.rollback()
        self.conn.close()

    def x(self, sql, params=()):
        return self.conn.exec_driver_sql(sql, params)

    def _insert_holder(self, name, liquid=0, lp=0, virtual=0, pik=0, abc_pik=0):
        self.x(
            """
            INSERT INTO tokenholders
                (member_name, liquid_tokens, LP_tokens, virtual_tokens, pik, abc_pik)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (name, liquid, lp, virtual, pik, abc_pik),
        )

    def _insert_delegation_trx(self, account, shares, vests, index=T_TRX_INDEX):
        self.x(
            """
            INSERT INTO trx (`index`, source, account, shares, vests, timestamp, status, share_type)
            VALUES (%s, %s, %s, %s, %s, NOW(), 'Valid', 'Delegation')
            """,
            (index, T_SOURCE, account, shares, vests),
        )


class GeneratedColumnTests(DBTestCase):
    def test_tokens_includes_virtual_tokens(self):
        self._insert_holder(T_HOLDER, liquid=10, lp=5, virtual=7)
        row = self.x(
            "SELECT tokens FROM tokenholders WHERE member_name = %s", (T_HOLDER,)
        ).fetchone()
        self.assertEqual(Decimal(str(row[0])), Decimal("22.000"))

    def test_management_outstanding_excludes_virtual(self):
        # outstanding base is tokens - virtual_tokens = real circulating supply
        self._insert_holder(T_HOLDER, liquid=10, lp=0, virtual=4)
        row = self.x(
            "SELECT SUM(tokens - virtual_tokens) FROM tokenholders WHERE member_name = %s",
            (T_HOLDER,),
        ).fetchone()
        self.assertEqual(Decimal(str(row[0])), Decimal("10.000"))

    def test_virtual_tokens_feed_dividend_accrual(self):
        # PIK accrual multiplies tokens (which includes virtual_tokens) by divs_per.
        self._insert_holder(T_HOLDER, liquid=0, lp=0, virtual=100)
        self.x(
            "UPDATE tokenholders SET accrued_dividends = accrued_dividends + tokens * %s "
            "WHERE member_name = %s",
            (Decimal("0.5"), T_HOLDER),
        )
        row = self.x(
            "SELECT accrued_dividends FROM tokenholders WHERE member_name = %s",
            (T_HOLDER,),
        ).fetchone()
        self.assertEqual(Decimal(str(row[0])), Decimal("50.000"))


class DelegationAccrualTests(DBTestCase):
    def test_clear_delegation_trx_zeroes_shares_and_vests(self):
        # Regression: the original bug zeroed only shares; update_member_db then
        # recomputed accrual from vests. Both must be zero.
        self._insert_delegation_trx(T_DEL, shares=10, vests=Decimal("20.000000"))
        clear_delegation_trx(self.conn, T_SOURCE, T_DEL)
        row = self.x(
            "SELECT shares, vests FROM trx WHERE `index` = %s AND source = %s",
            (T_TRX_INDEX, T_SOURCE),
        ).fetchone()
        self.assertEqual(row[0], 0)
        self.assertEqual(Decimal(str(row[1])), Decimal("0.000000"))

    def test_apply_active_delegations_is_atomic(self):
        # clear accrual AND set virtual_tokens together
        self._insert_delegation_trx(T_DEL2, shares=5, vests=Decimal("10.000000"))
        self._insert_holder(T_DEL2)
        apply_active_delegations(self.conn, T_SOURCE, [(T_DEL2, Decimal("12.345"))])

        trx_row = self.x(
            "SELECT shares, vests FROM trx WHERE `index` = %s AND source = %s",
            (T_TRX_INDEX, T_SOURCE),
        ).fetchone()
        self.assertEqual(trx_row[0], 0)
        self.assertEqual(Decimal(str(trx_row[1])), Decimal("0.000000"))

        vt = self.x(
            "SELECT virtual_tokens FROM tokenholders WHERE member_name = %s", (T_DEL2,)
        ).fetchone()
        self.assertEqual(Decimal(str(vt[0])), Decimal("12.345"))


class IssuanceGuardTests(DBTestCase):
    def test_selection_excludes_members_with_inflight_pending(self):
        self._insert_holder(T_GUARD, pik=Decimal("5.000"))
        before = {r[0] for r in select_issuable_balances(self.conn, "pik", "pik")}
        self.assertIn(T_GUARD, before)

        insert_pending_issuance(self.conn, T_GUARD, Decimal("5.000"), "pik")
        after = {r[0] for r in select_issuable_balances(self.conn, "pik", "pik")}
        self.assertNotIn(T_GUARD, after)

    def test_management_cap_counts_pending_plus_success(self):
        cap_sql = (
            "SELECT COALESCE(SUM(units), 0) FROM token_issuance_log "
            "WHERE rationale = 'Management' AND status IN ('SUCCESS', 'PENDING')"
        )
        before = Decimal(str(self.x(cap_sql).fetchone()[0]))
        insert_pending_issuance(self.conn, T_MGMT, Decimal("7.000"), "Management")
        after = Decimal(str(self.x(cap_sql).fetchone()[0]))
        self.assertEqual(after - before, Decimal("7.000"))


class ReconciliationTests(DBTestCase):
    def test_pending_management_confirmed_keeps_rationale(self):
        log_id = insert_pending_issuance(
            self.conn, T_MGMT, Decimal("12.345"), "Management"
        )
        reconcile_issuances(
            self.conn,
            [{"trx_id": "chain_mgmt_1", "recipient": T_MGMT, "units": Decimal("12.345")}],
        )
        row = self.x(
            "SELECT status, trx_id, rationale FROM token_issuance_log WHERE id = %s",
            (log_id,),
        ).fetchone()
        self.assertEqual(row[0], "SUCCESS")
        self.assertEqual(row[1], "chain_mgmt_1")
        self.assertEqual(row[2], "Management")  # rationale never rewritten

    def test_pending_pik_confirmed_debits_balance_once(self):
        self._insert_holder(T_PIK, pik=Decimal("12.345"))
        log_id = insert_pending_issuance(self.conn, T_PIK, Decimal("12.345"), "pik")
        reconcile_issuances(
            self.conn,
            [{"trx_id": "chain_pik_1", "recipient": T_PIK, "units": Decimal("12.345")}],
        )
        status = self.x(
            "SELECT status FROM token_issuance_log WHERE id = %s", (log_id,)
        ).fetchone()[0]
        pik = self.x(
            "SELECT pik FROM tokenholders WHERE member_name = %s", (T_PIK,)
        ).fetchone()[0]
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(Decimal(str(pik)), Decimal("0.000"))

    def test_stale_unmatched_pending_marked_failure(self):
        stale_ts = datetime.now(timezone.utc) - timedelta(hours=6)
        result = self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', NULL, 'Management', %s)
            """,
            (T_MGMT, Decimal("5.000"), stale_ts),
        )
        log_id = result.lastrowid
        reconcile_issuances(self.conn, [], now=datetime.now(timezone.utc))
        status = self.x(
            "SELECT status FROM token_issuance_log WHERE id = %s", (log_id,)
        ).fetchone()[0]
        self.assertEqual(status, "FAILURE")

    def test_recent_unmatched_pending_stays_pending(self):
        log_id = insert_pending_issuance(self.conn, T_MGMT, Decimal("5.000"), "Management")
        reconcile_issuances(self.conn, [], now=datetime.now(timezone.utc))
        status = self.x(
            "SELECT status FROM token_issuance_log WHERE id = %s", (log_id,)
        ).fetchone()[0]
        self.assertEqual(status, "PENDING")

    def test_orphan_chain_issue_logged_as_reconciled_only(self):
        orphan = {
            "trx_id": "chain_orphan_1",
            "recipient": "zz_vt_orphan",
            "units": Decimal("1.000"),
        }
        reconcile_issuances(self.conn, [orphan])
        row = self.x(
            "SELECT status, rationale FROM token_issuance_log WHERE trx_id = %s",
            ("chain_orphan_1",),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "SUCCESS")
        # audit-only: never attributed to Management or any balance rationale
        self.assertEqual(row[1], "reconciled")


if __name__ == "__main__":
    unittest.main()
