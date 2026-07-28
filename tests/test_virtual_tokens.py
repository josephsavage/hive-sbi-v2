"""Tests for delegation virtual_tokens, write-ahead token issuance intents,
Management 10% issuance, and chain-confirm reconciliation.

The pure-logic tests run anywhere. The DB-level tests connect to the local docker
MariaDB (the `sbi` database via config.json `databaseConnector2`) and run inside a
transaction that is rolled back, so they leave no residue. They skip automatically
when the database is unreachable (e.g. running on the host without the container).

Design note: every token issuance path creates a durable PENDING intent before
broadcast. Reconciliation uses chain timestamps to complete or fail those intents;
it never invents rationale from chain history.
"""

import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from hsbi_check_delegation import (
    apply_active_delegations,
    calculate_management_virtual_tokens,
    calculate_virtual_tokens,
    clear_delegation_trx,
    refresh_management_virtual_tokens,
)
from hivesbi.issuance_log import has_issuance_for_source
from hivesbi.parse_hist_op import (
    UNIT_CONVERSION_RATIONALE,
    _insert_pending_token_issuance,
    _mark_token_issuance_success,
)
from hsbi_token_snapshot import (
    TOKEN_PRECISION,
    _parse_engine_issue,
    calculate_management_issue_amount,
    fetch_recent_chain_issuance_scan,
    insert_pending_issuance,
    issue_balance_tokens,
    issue_management_tokens,
    log_issuance,
    reconcile_issuances,
    sync_tokenholders,
    warn_stuck_pending,
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


# --- fakes for issuance orchestration -----------------------------------------
class FakeIssuer:
    """Records issue() calls and returns a deterministic trx_id (or raises)."""

    def __init__(self, fail=False, trx_id="chain_fake"):
        self.fail = fail
        self.trx_id = trx_id
        self.calls = []

    def issue(self, recipient, amount):
        self.calls.append((recipient, float(amount)))
        if self.fail:
            raise RuntimeError("simulated broadcast failure")
        return {"trx_id": self.trx_id}


class _SameConnTxn:
    """Adapts one open test connection to the db2.engine.begin() context-manager
    protocol WITHOUT committing, so the surrounding rollback isolation holds."""

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        return False  # never commit; the test's outer transaction is rolled back


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn

    def begin(self):
        return _SameConnTxn(self._conn)


class FakeDB2:
    """Stand-in for the dataset db2 handle whose engine.begin() reuses one
    transaction-isolated connection."""

    def __init__(self, conn):
        self.engine = _FakeEngine(conn)


class _FakeHistoryAccount:
    def __init__(self, rows):
        self._rows = rows

    def history_reverse(self, only_ops=None):
        return iter(self._rows)


class FakeChainIssuer:
    """Issuer stand-in exposing a canned account history for scan tests."""

    def __init__(self, rows):
        self.hive_account = _FakeHistoryAccount(rows)


class PureLogicTests(unittest.TestCase):
    def test_virtual_tokens_are_decimal_hp_times_two(self):
        # No integer rounding: 0.001 HP delegated -> 0.002 virtual tokens.
        self.assertEqual(calculate_virtual_tokens("0.001"), Decimal("0.002"))
        self.assertEqual(calculate_virtual_tokens("123.4567"), Decimal("246.913"))

    def test_management_virtual_tokens_are_ten_percent_of_others(self):
        self.assertEqual(
            calculate_management_virtual_tokens(Decimal("1000.000")),
            Decimal("100.000"),
        )

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
        ts = datetime.now(timezone.utc)
        row = {
            "trx_id": "abc123",
            "timestamp": ts,
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
            {
                "trx_id": "abc123",
                "recipient": "josephsavage",
                "units": Decimal("12.345"),
                "timestamp": ts,
            },
        )

    def test_parse_engine_issue_ignores_non_issue(self):
        row = {
            "trx_id": "x",
            "op": ["custom_json", {"json": json.dumps({"contractName": "market"})}],
        }
        self.assertIsNone(_parse_engine_issue(row))

    def test_scan_coverage_extends_to_scan_time_for_quiet_issuer(self):
        # An issuer with no recent on-chain activity must still prove coverage up
        # to roughly the scan time: history_reverse starts at the account head,
        # so a successful fetch shows no newer ops exist. Old PENDING intents can
        # then be failed without waiting for fresh issuer activity.
        old_ts = datetime.now(timezone.utc) - timedelta(hours=48)
        rows = [{"trx_id": "old_op", "timestamp": old_ts, "op": ["custom_json", {}]}]
        scan = fetch_recent_chain_issuance_scan(FakeChainIssuer(rows))
        self.assertTrue(scan["complete"])
        self.assertGreater(
            scan["covered_until"],
            datetime.now(timezone.utc) - timedelta(minutes=10),
        )

    def test_scan_coverage_complete_for_empty_history(self):
        scan = fetch_recent_chain_issuance_scan(FakeChainIssuer([]))
        self.assertTrue(scan["complete"])
        self.assertEqual(
            scan["covered_since"], datetime.min.replace(tzinfo=timezone.utc)
        )
        self.assertIsNotNone(scan["covered_until"])

    def test_scan_incomplete_when_history_exceeds_limit(self):
        # The failure HISTORY_SCAN_LIMIT guards against: a burst longer than the
        # budget stops the walk before the cutoff, so absence is never proven, no
        # PENDING row can be failed, and issue_balance_tokens skips those members
        # every cycle. In the high-VP regime (cycle every ~15 min) a single window
        # has held ~1600 issuances, which is why the limit is sized well above it.
        recent = datetime.now(timezone.utc) - timedelta(minutes=1)
        rows = [
            {"trx_id": f"op_{i}", "timestamp": recent, "op": ["custom_json", {}]}
            for i in range(5)
        ]
        truncated = fetch_recent_chain_issuance_scan(FakeChainIssuer(rows), limit=3)
        self.assertFalse(truncated["complete"])
        self.assertIsNone(truncated["covered_since"])

        # Same history, budget above the burst: coverage is proven.
        sufficient = fetch_recent_chain_issuance_scan(FakeChainIssuer(rows), limit=10)
        self.assertTrue(sufficient["complete"])
        self.assertIsNotNone(sufficient["covered_since"])


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

    def _mgmt_state(self):
        """Current (outstanding real supply, cumulative Management issued)."""
        outstanding = Decimal(
            str(
                self.x(
                    "SELECT COALESCE(SUM(tokens - virtual_tokens), 0) FROM tokenholders "
                    "WHERE member_name <> 'sbi-tokens'"
                ).fetchone()[0]
            )
        )
        management_issued = Decimal(
            str(
                self.x(
                    "SELECT COALESCE(SUM(units), 0) FROM token_issuance_log "
                    "WHERE rationale = 'Management' AND status IN ('SUCCESS', 'PENDING')"
                ).fetchone()[0]
            )
        )
        return outstanding, management_issued


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

    def test_refresh_management_virtual_tokens_excludes_management(self):
        self.x("UPDATE tokenholders SET virtual_tokens = 0")
        self._insert_holder("zz_vt_other_a", virtual=Decimal("400.000"))
        self._insert_holder("zz_vt_other_b", virtual=Decimal("600.000"))
        self.x(
            """
            INSERT INTO tokenholders (member_name, virtual_tokens)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE virtual_tokens = VALUES(virtual_tokens)
            """,
            (T_MGMT, Decimal("999.000")),
        )

        refreshed = refresh_management_virtual_tokens(self.conn)

        self.assertEqual(refreshed, Decimal("100.000"))
        row = self.x(
            "SELECT virtual_tokens FROM tokenholders WHERE member_name = %s",
            (T_MGMT,),
        ).fetchone()
        self.assertEqual(Decimal(str(row[0])), Decimal("100.000"))

    def test_refresh_management_virtual_tokens_upserts_missing_management_row(self):
        self.x("UPDATE tokenholders SET virtual_tokens = 0")
        self.x("DELETE FROM tokenholders WHERE member_name = %s", (T_MGMT,))
        self._insert_holder("zz_vt_other_c", virtual=Decimal("1000.000"))

        refreshed = refresh_management_virtual_tokens(self.conn)

        self.assertEqual(refreshed, Decimal("100.000"))
        row = self.x(
            "SELECT virtual_tokens FROM tokenholders WHERE member_name = %s",
            (T_MGMT,),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(Decimal(str(row[0])), Decimal("100.000"))

    def test_refresh_management_preserves_own_delegation_virtual(self):
        # If the management account itself delegates HP, its delegation-derived
        # virtual tokens are preserved on top of the derived 10% allocation
        # rather than being overwritten by the refresh.
        self.x("UPDATE tokenholders SET virtual_tokens = 0")
        self._insert_holder("zz_vt_other_d", virtual=Decimal("1000.000"))

        refreshed = refresh_management_virtual_tokens(self.conn, Decimal("50.000"))

        self.assertEqual(refreshed, Decimal("150.000"))
        row = self.x(
            "SELECT virtual_tokens FROM tokenholders WHERE member_name = %s",
            (T_MGMT,),
        ).fetchone()
        self.assertEqual(Decimal(str(row[0])), Decimal("150.000"))


class ImmediateBalanceIssuanceTests(DBTestCase):
    """pik / abc_pik issuance uses write-ahead PENDING intents."""

    def test_success_zeroes_balance_and_logs_success(self):
        self._insert_holder(T_PIK, pik=Decimal("5.000"))
        issuer = FakeIssuer(trx_id="chain_pik_ok")
        issue_balance_tokens(FakeDB2(self.conn), issuer, "pik", "pik")

        self.assertEqual(issuer.calls, [(T_PIK, 5.0)])
        pik = self.x(
            "SELECT pik FROM tokenholders WHERE member_name = %s", (T_PIK,)
        ).fetchone()[0]
        self.assertEqual(Decimal(str(pik)), Decimal("0.000"))
        row = self.x(
            "SELECT status, trx_id, rationale FROM token_issuance_log "
            "WHERE recipient = %s AND trx_id = %s",
            (T_PIK, "chain_pik_ok"),
        ).fetchone()
        self.assertEqual(row[0], "SUCCESS")
        self.assertEqual(row[2], "pik")

    def test_failure_keeps_balance_and_leaves_pending_for_reconciliation(self):
        self._insert_holder(T_PIK, pik=Decimal("5.000"))
        issuer = FakeIssuer(fail=True)
        issue_balance_tokens(FakeDB2(self.conn), issuer, "pik", "pik")

        pik = self.x(
            "SELECT pik FROM tokenholders WHERE member_name = %s", (T_PIK,)
        ).fetchone()[0]
        self.assertEqual(Decimal(str(pik)), Decimal("5.000"))  # intact -> retried

        last = self.x(
            "SELECT status, error_message FROM token_issuance_log WHERE recipient = %s "
            "AND rationale = 'pik' ORDER BY id DESC LIMIT 1",
            (T_PIK,),
        ).fetchone()
        self.assertEqual(last[0], "PENDING")
        self.assertIn("simulated broadcast failure", last[1])

        pending = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE recipient = %s "
            "AND status = 'PENDING'",
            (T_PIK,),
        ).fetchone()[0]
        self.assertEqual(int(pending), 1)

    def test_existing_pending_blocks_duplicate_balance_issuance(self):
        self._insert_holder(T_PIK, pik=Decimal("5.000"))
        insert_pending_issuance(self.conn, T_PIK, Decimal("5.000"), "pik")
        issuer = FakeIssuer(trx_id="chain_should_not_issue")

        issue_balance_tokens(FakeDB2(self.conn), issuer, "pik", "pik")

        self.assertEqual(issuer.calls, [])

    def test_abc_pik_uses_its_own_column_and_rationale(self):
        self._insert_holder(T_PIK, abc_pik=Decimal("3.000"))
        issuer = FakeIssuer(trx_id="chain_abc_ok")
        issue_balance_tokens(
            FakeDB2(self.conn), issuer, "Pending Balance Conversion", "abc_pik"
        )
        abc = self.x(
            "SELECT abc_pik FROM tokenholders WHERE member_name = %s", (T_PIK,)
        ).fetchone()[0]
        self.assertEqual(Decimal(str(abc)), Decimal("0.000"))
        rationale = self.x(
            "SELECT rationale FROM token_issuance_log WHERE trx_id = %s",
            ("chain_abc_ok",),
        ).fetchone()[0]
        self.assertEqual(rationale, "Pending Balance Conversion")


class ManagementIssuanceTests(DBTestCase):
    def test_issues_exactly_the_capped_amount_then_converges(self):
        outstanding, management_issued = self._mgmt_state()
        expected = calculate_management_issue_amount(outstanding, management_issued)

        issuer = FakeIssuer(trx_id="chain_mgmt_ok")
        issue_management_tokens(FakeDB2(self.conn), issuer)

        if expected < TOKEN_PRECISION:
            # Cap already met for the current DB state: nothing should issue.
            self.assertEqual(issuer.calls, [])
        else:
            self.assertEqual(issuer.calls, [(T_MGMT, float(expected))])
            row = self.x(
                "SELECT status, units, rationale FROM token_issuance_log "
                "WHERE trx_id = %s",
                ("chain_mgmt_ok",),
            ).fetchone()
            self.assertEqual(row[0], "SUCCESS")
            self.assertEqual(Decimal(str(row[1])), expected)
            self.assertEqual(row[2], "Management")

        # A second back-to-back run must not issue again (cap now counts the first,
        # whether it issued or skipped) — the write-ahead cap prevents double-mint.
        issuer2 = FakeIssuer(trx_id="chain_mgmt_ok2")
        issue_management_tokens(FakeDB2(self.conn), issuer2)
        self.assertEqual(issuer2.calls, [])

    def test_management_cap_counts_pending_plus_success(self):
        cap_sql = (
            "SELECT COALESCE(SUM(units), 0) FROM token_issuance_log "
            "WHERE rationale = 'Management' AND status IN ('SUCCESS', 'PENDING')"
        )
        before = Decimal(str(self.x(cap_sql).fetchone()[0]))
        insert_pending_issuance(self.conn, T_MGMT, Decimal("7.000"), "Management")
        after = Decimal(str(self.x(cap_sql).fetchone()[0]))
        self.assertEqual(after - before, Decimal("7.000"))


class UnitConversionIssuanceTests(DBTestCase):
    def test_unit_conversion_separates_source_and_issue_trx_ids(self):
        source_trx = "6fda651869153df5fefb8080c1cc985ee155261b"
        issue_trx = "af4cedd8f982efd512e890912b067a6403e891b7"
        log_id = _insert_pending_token_issuance(
            self.conn,
            T_PIK,
            Decimal("10.000"),
            UNIT_CONVERSION_RATIONALE,
            source_trx_id=source_trx,
        )

        _mark_token_issuance_success(self.conn, log_id, issue_trx)

        row = self.x(
            """
            SELECT status, trx_id, source_trx_id, rationale
            FROM token_issuance_log
            WHERE id = %s
            """,
            (log_id,),
        ).fetchone()
        self.assertEqual(row[0], "SUCCESS")
        self.assertEqual(row[1], issue_trx)
        self.assertEqual(row[2], source_trx)
        self.assertEqual(row[3], UNIT_CONVERSION_RATIONALE)

    def test_unit_conversion_issue_trx_does_not_reconcile_duplicate(self):
        source_trx = "6fda651869153df5fefb8080c1cc985ee155261b"
        issue_trx = "af4cedd8f982efd512e890912b067a6403e891b7"
        now = datetime.now(timezone.utc)
        log_id = _insert_pending_token_issuance(
            self.conn,
            T_PIK,
            Decimal("10.000"),
            UNIT_CONVERSION_RATIONALE,
            source_trx_id=source_trx,
        )
        _mark_token_issuance_success(self.conn, log_id, issue_trx)

        reconcile_issuances(
            self.conn,
            [
                {
                    "trx_id": issue_trx,
                    "recipient": T_PIK,
                    "units": Decimal("10.000"),
                    "timestamp": now,
                }
            ],
        )

        duplicate_count = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE trx_id = %s",
            (issue_trx,),
        ).fetchone()[0]
        reconciled_count = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE rationale = 'reconciled'"
        ).fetchone()[0]
        self.assertEqual(int(duplicate_count), 1)
        self.assertEqual(int(reconciled_count), 0)

    def test_source_trx_guard_blocks_pending_and_success_not_failure(self):
        # A reprocessed transfer op must not mint the same source transaction
        # twice: PENDING (in flight) and SUCCESS both block, a proven FAILURE
        # leaves the retry path open.
        source_trx = "zz_vt_source_guard_1"
        self.assertFalse(
            has_issuance_for_source(self.conn, source_trx, UNIT_CONVERSION_RATIONALE)
        )

        log_id = _insert_pending_token_issuance(
            self.conn,
            T_PIK,
            Decimal("1.000"),
            UNIT_CONVERSION_RATIONALE,
            source_trx_id=source_trx,
        )
        self.assertTrue(
            has_issuance_for_source(self.conn, source_trx, UNIT_CONVERSION_RATIONALE)
        )

        _mark_token_issuance_success(self.conn, log_id, "chain_src_guard_1")
        self.assertTrue(
            has_issuance_for_source(self.conn, source_trx, UNIT_CONVERSION_RATIONALE)
        )

        self.x(
            "UPDATE token_issuance_log SET status = 'FAILURE' WHERE id = %s",
            (log_id,),
        )
        self.assertFalse(
            has_issuance_for_source(self.conn, source_trx, UNIT_CONVERSION_RATIONALE)
        )


class SyncTokenholdersTests(DBTestCase):
    def test_sync_zeroes_then_upserts_liquid_from_chain(self):
        self._insert_holder(T_HOLDER, liquid=Decimal("1.000"))
        fake_holders = [{"account": T_HOLDER, "balance": Decimal("42.000")}]
        with patch(
            "hsbi_token_snapshot.get_tokenholders", return_value=fake_holders
        ):
            sync_tokenholders(FakeDB2(self.conn))
        liquid = self.x(
            "SELECT liquid_tokens FROM tokenholders WHERE member_name = %s",
            (T_HOLDER,),
        ).fetchone()[0]
        self.assertEqual(Decimal(str(liquid)), Decimal("42.000"))


class ReconciliationTests(DBTestCase):
    def test_pending_management_confirmed_keeps_rationale(self):
        now = datetime.now(timezone.utc)
        log_id = insert_pending_issuance(
            self.conn, T_MGMT, Decimal("12.345"), "Management", issued_at=now
        )
        reconcile_issuances(
            self.conn,
            [
                {
                    "trx_id": "chain_mgmt_1",
                    "recipient": T_MGMT,
                    "units": Decimal("12.345"),
                    "timestamp": now + timedelta(seconds=30),
                }
            ],
        )
        row = self.x(
            "SELECT status, trx_id, rationale FROM token_issuance_log WHERE id = %s",
            (log_id,),
        ).fetchone()
        self.assertEqual(row[0], "SUCCESS")
        self.assertEqual(row[1], "chain_mgmt_1")
        self.assertEqual(row[2], "Management")  # rationale never rewritten

    def test_confirmed_pik_chain_issue_not_attributed_to_management(self):
        # josephsavage receives BOTH a pik dividend and a Management issuance of the
        # same amount in one cycle. The pik issuance is already logged SUCCESS with
        # its real trx_id, so it is excluded; the Management PENDING must match the
        # *Management* chain op, never the pik one.
        now = datetime.now(timezone.utc)
        log_issuance(
            self.conn,
            "chain_pik_real",
            T_MGMT,
            Decimal("5.000"),
            "SUCCESS",
            "pik",
            issued_at=now - timedelta(minutes=2),
        )
        mgmt_id = insert_pending_issuance(
            self.conn,
            T_MGMT,
            Decimal("5.000"),
            "Management",
            issued_at=now - timedelta(minutes=1),
        )
        chain = [
            {
                "trx_id": "chain_pik_real",
                "recipient": T_MGMT,
                "units": Decimal("5.000"),
                "timestamp": now - timedelta(minutes=2),
            },
            {
                "trx_id": "chain_mgmt_real",
                "recipient": T_MGMT,
                "units": Decimal("5.000"),
                "timestamp": now - timedelta(minutes=1),
            },
        ]
        reconcile_issuances(self.conn, chain)

        row = self.x(
            "SELECT status, trx_id FROM token_issuance_log WHERE id = %s", (mgmt_id,)
        ).fetchone()
        self.assertEqual(row[0], "SUCCESS")
        self.assertEqual(row[1], "chain_mgmt_real")
        # Neither chain op should be re-logged as an out-of-band 'reconciled' row.
        orphans = self.x(
            "SELECT COUNT(*) FROM token_issuance_log "
            "WHERE rationale = 'reconciled' AND recipient = %s",
            (T_MGMT,),
        ).fetchone()[0]
        self.assertEqual(int(orphans), 0)

    def test_uncaptured_pik_chain_issue_updates_existing_log(self):
        # A pik issuance whose on-chain trx_id was not captured (logged 'N/A') must
        # not be double-logged as an out-of-band 'reconciled' issuance.
        now = datetime.now(timezone.utc)
        log_issuance(
            self.conn,
            "N/A",
            T_PIK,
            Decimal("3.000"),
            "SUCCESS",
            "pik",
            issued_at=now,
        )
        chain = [
            {
                "trx_id": "chain_pik_uncaptured",
                "recipient": T_PIK,
                "units": Decimal("3.000"),
                "timestamp": now + timedelta(seconds=5),
            }
        ]
        reconcile_issuances(self.conn, chain)
        updated = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE trx_id = %s AND rationale = 'pik'",
            ("chain_pik_uncaptured",),
        ).fetchone()[0]
        self.assertEqual(int(updated), 1)
        reconciled = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE rationale = 'reconciled'"
        ).fetchone()[0]
        self.assertEqual(int(reconciled), 0)

    def test_uncaptured_success_claims_chain_issue_before_pending_row(self):
        # A SUCCESS row logged 'N/A' and a PENDING row share recipient and units,
        # and both are in-window for the single chain issue. The chain issue belongs
        # to the SUCCESS row (it is already on chain); handing it to the PENDING row
        # would stamp that row with another issuance's trx_id and strand the 'N/A'
        # row on its placeholder forever.
        now = datetime.now(timezone.utc)
        uncaptured = log_issuance(
            self.conn,
            "N/A",
            T_PIK,
            Decimal("4.000"),
            "SUCCESS",
            "pik",
            issued_at=now - timedelta(minutes=10),
        )
        uncaptured_id = self.x(
            "SELECT id FROM token_issuance_log WHERE trx_id = 'N/A' "
            "AND recipient = %s AND units = %s",
            (T_PIK, Decimal("4.000")),
        ).fetchone()[0]
        pending_id = insert_pending_issuance(
            self.conn,
            T_PIK,
            Decimal("4.000"),
            "Pending Balance Conversion",
            issued_at=now - timedelta(minutes=9),
        )
        chain = [
            {
                "trx_id": "chain_uncaptured_wins",
                "recipient": T_PIK,
                "units": Decimal("4.000"),
                "timestamp": now - timedelta(minutes=8),
            }
        ]
        reconcile_issuances(self.conn, chain)

        self.assertEqual(
            self.x(
                "SELECT trx_id FROM token_issuance_log WHERE id = %s", (uncaptured_id,)
            ).fetchone()[0],
            "chain_uncaptured_wins",
        )
        # The PENDING row must not have absorbed the same chain issue.
        pending_row = self.x(
            "SELECT status, trx_id FROM token_issuance_log WHERE id = %s", (pending_id,)
        ).fetchone()
        self.assertEqual(pending_row[0], "PENDING")
        self.assertEqual(pending_row[1], "PENDING")
        # And no chain issue may ever be re-logged as a new row.
        self.assertEqual(
            int(
                self.x(
                    "SELECT COUNT(*) FROM token_issuance_log WHERE trx_id = %s",
                    ("chain_uncaptured_wins",),
                ).fetchone()[0]
            ),
            1,
        )

    def test_uncaptured_pass_leaves_pending_its_own_chain_issue(self):
        # Mirror of the test above, and the regression guard for the reorder: with
        # 'N/A' completion running first it must consume only the issue that is
        # closest to its own row, never sweep the pool. Both rows share recipient
        # and units and BOTH chain issues fall inside BOTH match windows, so the
        # separation comes from ordering + proximity, not from disjoint windows.
        base = datetime.now(timezone.utc) - timedelta(minutes=20)
        log_issuance(
            self.conn,
            "N/A",
            T_PIK,
            Decimal("7.000"),
            "SUCCESS",
            "pik",
            issued_at=base,
        )
        uncaptured_id = self.x(
            "SELECT id FROM token_issuance_log WHERE trx_id = 'N/A' "
            "AND recipient = %s AND units = %s",
            (T_PIK, Decimal("7.000")),
        ).fetchone()[0]
        pending_id = insert_pending_issuance(
            self.conn,
            T_PIK,
            Decimal("7.000"),
            "Pending Balance Conversion",
            issued_at=base + timedelta(minutes=3),
        )
        chain = [
            {
                "trx_id": "chain_for_uncaptured",
                "recipient": T_PIK,
                "units": Decimal("7.000"),
                "timestamp": base + timedelta(minutes=1),
            },
            {
                "trx_id": "chain_for_pending",
                "recipient": T_PIK,
                "units": Decimal("7.000"),
                "timestamp": base + timedelta(minutes=4),
            },
        ]
        reconcile_issuances(self.conn, chain)

        self.assertEqual(
            self.x(
                "SELECT trx_id FROM token_issuance_log WHERE id = %s", (uncaptured_id,)
            ).fetchone()[0],
            "chain_for_uncaptured",
        )
        pending_row = self.x(
            "SELECT status, trx_id FROM token_issuance_log WHERE id = %s", (pending_id,)
        ).fetchone()
        self.assertEqual(pending_row[0], "SUCCESS")
        self.assertEqual(pending_row[1], "chain_for_pending")
        # Two issuances in, two rows out — neither chain op created a third.
        self.assertEqual(
            int(
                self.x(
                    "SELECT COUNT(*) FROM token_issuance_log "
                    "WHERE recipient = %s AND units = %s",
                    (T_PIK, Decimal("7.000")),
                ).fetchone()[0]
            ),
            2,
        )

    def test_second_chain_issue_cannot_complete_the_same_row_twice(self):
        # Two in-window chain issues, one uncaptured row. The first completes it;
        # the second must fall through to the recognise-and-report branch, never
        # inserting a row. This is the invariant PR #138 violated.
        base = datetime.now(timezone.utc) - timedelta(minutes=20)
        log_issuance(
            self.conn,
            "N/A",
            T_PIK,
            Decimal("8.000"),
            "SUCCESS",
            "pik",
            issued_at=base,
        )
        chain = [
            {
                "trx_id": "chain_first",
                "recipient": T_PIK,
                "units": Decimal("8.000"),
                "timestamp": base + timedelta(minutes=1),
            },
            {
                "trx_id": "chain_second",
                "recipient": T_PIK,
                "units": Decimal("8.000"),
                "timestamp": base + timedelta(minutes=2),
            },
        ]
        reconcile_issuances(self.conn, chain)

        rows = self.x(
            "SELECT trx_id FROM token_issuance_log "
            "WHERE recipient = %s AND units = %s",
            (T_PIK, Decimal("8.000")),
        ).fetchall()
        self.assertEqual([r[0] for r in rows], ["chain_first"])

    def test_warn_stuck_pending_flags_only_old_pending_rows(self):
        # A stuck PENDING row blocks its (recipient, rationale) in
        # issue_balance_tokens and produces no error of its own, so the alert is
        # the only signal that a member has stopped being paid.
        now = datetime.now(timezone.utc)
        baseline = warn_stuck_pending(self.conn)

        insert_pending_issuance(
            self.conn, T_PIK, Decimal("1.000"), "pik", issued_at=now - timedelta(hours=7)
        )
        insert_pending_issuance(
            self.conn, T_GUARD, Decimal("1.000"), "pik", issued_at=now - timedelta(hours=1)
        )
        log_issuance(
            self.conn,
            "chain_old_success",
            T_HOLDER,
            Decimal("1.000"),
            "SUCCESS",
            "pik",
            issued_at=now - timedelta(hours=7),
        )

        # Only the old PENDING row qualifies: recent PENDING and old SUCCESS do not.
        self.assertEqual(warn_stuck_pending(self.conn) - baseline, 1)

    def test_stale_unmatched_pending_stays_pending_without_covered_scan(self):
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
        reconcile_issuances(self.conn, [])
        status = self.x(
            "SELECT status FROM token_issuance_log WHERE id = %s", (log_id,)
        ).fetchone()[0]
        self.assertEqual(status, "PENDING")

    def test_stale_unmatched_pending_marked_failure_when_scan_covers_row(self):
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
        # Coverage must span the row's whole possible match window:
        # [issued_at - MATCH_CLOCK_SKEW, issued_at + CHAIN_MATCH_WINDOW].
        reconcile_issuances(
            self.conn,
            [],
            covered_since=stale_ts - timedelta(minutes=6),
            covered_until=stale_ts + timedelta(minutes=31),
            scan_complete=True,
        )
        status = self.x(
            "SELECT status FROM token_issuance_log WHERE id = %s", (log_id,)
        ).fetchone()[0]
        self.assertEqual(status, "FAILURE")

    def test_recent_unmatched_pending_stays_pending(self):
        log_id = insert_pending_issuance(self.conn, T_MGMT, Decimal("5.000"), "Management")
        reconcile_issuances(self.conn, [])
        status = self.x(
            "SELECT status FROM token_issuance_log WHERE id = %s", (log_id,)
        ).fetchone()[0]
        self.assertEqual(status, "PENDING")

    def test_orphan_chain_issue_reported_not_inserted(self):
        orphan = {
            "trx_id": "chain_orphan_1",
            "recipient": "zz_vt_orphan",
            "units": Decimal("1.000"),
            "timestamp": datetime.now(timezone.utc),
        }
        reconcile_issuances(self.conn, [orphan])
        count = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE trx_id = %s",
            ("chain_orphan_1",),
        ).fetchone()[0]
        self.assertEqual(int(count), 0)

    def test_reconciliation_completes_pik_pending_and_debits_balance(self):
        now = datetime.now(timezone.utc)
        self._insert_holder(T_PIK, pik=Decimal("5.000"))
        log_id = insert_pending_issuance(
            self.conn, T_PIK, Decimal("5.000"), "pik", issued_at=now
        )

        reconcile_issuances(
            self.conn,
            [
                {
                    "trx_id": "chain_pik_reconciled",
                    "recipient": T_PIK,
                    "units": Decimal("5.000"),
                    "timestamp": now + timedelta(seconds=10),
                }
            ],
        )

        row = self.x(
            "SELECT status, trx_id FROM token_issuance_log WHERE id = %s",
            (log_id,),
        ).fetchone()
        pik = self.x(
            "SELECT pik FROM tokenholders WHERE member_name = %s", (T_PIK,)
        ).fetchone()[0]
        self.assertEqual(row[0], "SUCCESS")
        self.assertEqual(row[1], "chain_pik_reconciled")
        self.assertEqual(Decimal(str(pik)), Decimal("0.000"))

    def test_reconciliation_uses_chain_timestamp_not_server_now(self):
        # An intent hours in the past still matches: only the chain timestamp
        # relative to the intent timestamp matters, never the server clock.
        intent_at = datetime(2026, 6, 21, 12, 0, tzinfo=timezone.utc)
        log_id = insert_pending_issuance(
            self.conn, T_MGMT, Decimal("5.000"), "Management", issued_at=intent_at
        )

        reconcile_issuances(
            self.conn,
            [
                {
                    "trx_id": "chain_timestamp_authority",
                    "recipient": T_MGMT,
                    "units": Decimal("5.000"),
                    "timestamp": intent_at + timedelta(minutes=10),
                }
            ],
        )

        row = self.x(
            "SELECT status, trx_id FROM token_issuance_log WHERE id = %s",
            (log_id,),
        ).fetchone()
        self.assertEqual(row[0], "SUCCESS")
        self.assertEqual(row[1], "chain_timestamp_authority")

    def test_nearest_timestamp_match_among_management_pending(self):
        # Two Management PENDING rows of the same amount; the chain op confirms the
        # one nearest in time, the other stays PENDING.
        older_ts = datetime.now(timezone.utc) - timedelta(minutes=20)
        newer_ts = datetime.now(timezone.utc) - timedelta(minutes=1)
        old_id = self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', NULL, 'Management', %s)
            """,
            (T_MGMT, Decimal("5.000"), older_ts),
        ).lastrowid
        new_id = self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', NULL, 'Management', %s)
            """,
            (T_MGMT, Decimal("5.000"), newer_ts),
        ).lastrowid

        reconcile_issuances(
            self.conn,
            [
                {
                    "trx_id": "chain_nearest_1",
                    "recipient": T_MGMT,
                    "units": Decimal("5.000"),
                    "timestamp": newer_ts + timedelta(seconds=30),
                }
            ],
        )

        rows = self.x(
            """
            SELECT id, status, trx_id
            FROM token_issuance_log
            WHERE id IN (%s, %s)
            ORDER BY id
            """,
            (old_id, new_id),
        ).fetchall()
        self.assertEqual(rows[0][1], "PENDING")
        self.assertEqual(rows[1][1], "SUCCESS")
        self.assertEqual(rows[1][2], "chain_nearest_1")

    def test_ambiguous_equal_timestamp_distance_stays_pending(self):
        # issued_at is a second-precision TIMESTAMP column, so the tie must be
        # constructed on whole seconds to survive the DB round-trip. Even with
        # full scan coverage, neither contested row may be resolved or failed.
        now = datetime.now(timezone.utc).replace(microsecond=0)
        first_id = self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', NULL, 'Management', %s)
            """,
            (T_MGMT, Decimal("5.000"), now - timedelta(minutes=1)),
        ).lastrowid
        second_id = self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', NULL, 'Management', %s)
            """,
            (T_MGMT, Decimal("5.000"), now + timedelta(minutes=1)),
        ).lastrowid

        reconcile_issuances(
            self.conn,
            [
                {
                    "trx_id": "chain_ambiguous_1",
                    "recipient": T_MGMT,
                    "units": Decimal("5.000"),
                    "timestamp": now,
                }
            ],
            covered_since=now - timedelta(hours=1),
            covered_until=now + timedelta(hours=1),
            scan_complete=True,
        )

        statuses = self.x(
            """
            SELECT status
            FROM token_issuance_log
            WHERE id IN (%s, %s)
            ORDER BY id
            """,
            (first_id, second_id),
        ).fetchall()
        self.assertEqual([row[0] for row in statuses], ["PENDING", "PENDING"])

    def test_contested_pending_not_failed_then_resolves_next_pass(self):
        # Chain issue X matches intent A (nearest); sibling intent B of the same
        # recipient+units is contested by X, so a covering scan must NOT fail B
        # this pass — X may actually be B's broadcast. Once A's SUCCESS records
        # X's trx_id, the next pass excludes X, proves absence, and fails B, so
        # a contested row never blocks its member forever.
        base = datetime.now(timezone.utc).replace(microsecond=0)
        a_ts = base - timedelta(hours=2)
        b_ts = a_ts + timedelta(minutes=2)
        a_id = insert_pending_issuance(
            self.conn, T_MGMT, Decimal("5.000"), "Management", issued_at=a_ts
        )
        b_id = insert_pending_issuance(
            self.conn, T_MGMT, Decimal("5.000"), "Management", issued_at=b_ts
        )
        chain = [
            {
                "trx_id": "chain_contested_1",
                "recipient": T_MGMT,
                "units": Decimal("5.000"),
                "timestamp": a_ts + timedelta(seconds=30),
            }
        ]
        coverage = {
            "covered_since": a_ts - timedelta(minutes=10),
            "covered_until": base,
            "scan_complete": True,
        }

        reconcile_issuances(self.conn, chain, **coverage)

        rows = self.x(
            "SELECT id, status FROM token_issuance_log WHERE id IN (%s, %s) ORDER BY id",
            (a_id, b_id),
        ).fetchall()
        self.assertEqual(rows[0][1], "SUCCESS")
        self.assertEqual(rows[1][1], "PENDING")  # contested, not FAILURE

        reconcile_issuances(self.conn, chain, **coverage)

        b_status = self.x(
            "SELECT status FROM token_issuance_log WHERE id = %s", (b_id,)
        ).fetchone()[0]
        self.assertEqual(b_status, "FAILURE")


if __name__ == "__main__":
    unittest.main()
