"""Tests for delegation virtual_tokens, per-member (pik / abc_pik) issuance, the
Management 10% write-ahead issuance, and chain-confirm reconciliation.

The pure-logic tests run anywhere. The DB-level tests connect to the local docker
MariaDB (the `sbi` database via config.json `databaseConnector2`) and run inside a
transaction that is rolled back, so they leave no residue. They skip automatically
when the database is unreachable (e.g. running on the host without the container).

Design note (PR #138 review): per-member pik / abc_pik issuance is immediate and
self-healing — a failed broadcast leaves the balance for retry next cycle and never
creates a PENDING row. Only the capped Management issuance uses the write-ahead
PENDING protocol, so reconciliation is rationale-scoped to Management.
"""

import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from hsbi_check_delegation import (
    apply_active_delegations,
    calculate_virtual_tokens,
    clear_delegation_trx,
)
from hsbi_token_snapshot import (
    TOKEN_PRECISION,
    _parse_engine_issue,
    calculate_management_issue_amount,
    insert_pending_issuance,
    issue_balance_tokens,
    issue_management_tokens,
    log_issuance,
    reconcile_issuances,
    sync_tokenholders,
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


class ImmediateBalanceIssuanceTests(DBTestCase):
    """pik / abc_pik issuance is immediate and self-healing — no PENDING guard."""

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

    def test_failure_keeps_balance_for_retry_and_creates_no_pending(self):
        # The whole point of reverting the write-ahead pik path: a transient failure
        # must be temporary (retried next cycle), never a permanent PENDING block.
        self._insert_holder(T_PIK, pik=Decimal("5.000"))
        issuer = FakeIssuer(fail=True)
        issue_balance_tokens(FakeDB2(self.conn), issuer, "pik", "pik")

        pik = self.x(
            "SELECT pik FROM tokenholders WHERE member_name = %s", (T_PIK,)
        ).fetchone()[0]
        self.assertEqual(Decimal(str(pik)), Decimal("5.000"))  # intact -> retried

        last = self.x(
            "SELECT status FROM token_issuance_log WHERE recipient = %s "
            "AND rationale = 'pik' ORDER BY id DESC LIMIT 1",
            (T_PIK,),
        ).fetchone()
        self.assertEqual(last[0], "FAILURE")

        pending = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE recipient = %s "
            "AND status = 'PENDING'",
            (T_PIK,),
        ).fetchone()[0]
        self.assertEqual(int(pending), 0)

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

    def test_confirmed_pik_chain_issue_not_attributed_to_management(self):
        # josephsavage receives BOTH a pik dividend and a Management issuance of the
        # same amount in one cycle. The pik issuance is already logged SUCCESS with
        # its real trx_id, so it is excluded; the Management PENDING must match the
        # *Management* chain op, never the pik one.
        log_issuance(
            self.conn, "chain_pik_real", T_MGMT, Decimal("5.000"), "SUCCESS", "pik"
        )
        mgmt_id = insert_pending_issuance(self.conn, T_MGMT, Decimal("5.000"), "Management")
        now = datetime.now(timezone.utc)
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
        reconcile_issuances(self.conn, chain, now=now)

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

    def test_unlogged_pik_chain_issue_suppressed_from_orphans(self):
        # A pik issuance whose on-chain trx_id was not captured (logged 'N/A') must
        # not be double-logged as an out-of-band 'reconciled' issuance.
        log_issuance(self.conn, "N/A", T_PIK, Decimal("3.000"), "SUCCESS", "pik")
        chain = [
            {"trx_id": "chain_pik_uncaptured", "recipient": T_PIK, "units": Decimal("3.000")}
        ]
        reconcile_issuances(self.conn, chain, now=datetime.now(timezone.utc))
        orphan = self.x(
            "SELECT COUNT(*) FROM token_issuance_log WHERE trx_id = %s",
            ("chain_pik_uncaptured",),
        ).fetchone()[0]
        self.assertEqual(int(orphan), 0)

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
        reconcile_issuances(self.conn, [], now=datetime.now(timezone.utc))
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
        reconcile_issuances(
            self.conn,
            [],
            now=datetime.now(timezone.utc),
            covered_since=stale_ts - timedelta(minutes=1),
            scan_complete=True,
        )
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
            now=datetime.now(timezone.utc),
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


if __name__ == "__main__":
    unittest.main()
