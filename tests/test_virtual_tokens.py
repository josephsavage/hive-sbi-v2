"""Tests for delegation virtual_tokens, write-ahead token issuance intents,
Management 10% issuance, and the settling of unconfirmed issuances.

The pure-logic tests run anywhere. The DB-level tests connect to the local docker
MariaDB (the `sbi` database via config.json `databaseConnector2`) and run inside a
transaction that is rolled back, so they leave no residue. They skip automatically
when the database is unreachable (e.g. running on the host without the container).

Design note: every token issuance path creates a durable PENDING intent before
broadcast. Nothing asks the chain what became of that intent. A row is failed only
when its own recorded broadcast error proves the operation was never included;
anything else is held PENDING for an operator. No rationale is ever invented from
chain history and no row is ever inserted from chain data.
"""

import unittest
import unittest.mock
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
import hivesbi.issue as issue_module
from hivesbi.issuance_log import (
    fail_pending_with_proven_non_inclusion,
    has_issuance_for_source,
    never_reached_chain,
    record_broadcast_error,
)
from hivesbi.parse_hist_op import (
    UNIT_CONVERSION_RATIONALE,
    _insert_pending_token_issuance,
    _mark_token_issuance_success,
)
from hsbi_token_snapshot import (
    TOKEN_PRECISION,
    calculate_management_issue_amount,
    insert_pending_issuance,
    issue_balance_tokens,
    issue_management_tokens,
    log_issuance,
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

    def __init__(self, fail=False, trx_id="chain_fake", error="simulated broadcast failure"):
        self.fail = fail
        self.trx_id = trx_id
        self.error = error
        self.calls = []

    def issue(self, recipient, amount):
        self.calls.append((recipient, float(amount)))
        if self.fail:
            raise RuntimeError(self.error)
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

    def test_block_limit_error_is_recognized_as_never_broadcast(self):
        self.assertTrue(
            never_reached_chain(
                "Assert Exception:insert_info.first->second <= "
                "HIVE_CUSTOM_OP_BLOCK_LIMIT: Account hivesbi already submitted 5 "
                "custom json operation(s) this block."
            )
        )

    def test_no_chain_history_client_is_reachable(self):
        # Nothing may settle a token outcome from a runtime-chosen node's answer.
        # PR #138 scanned the issuer's whole custom_json history and duplicated
        # every Unit Conversion row; #140's first draft narrowed that to a per-row
        # Hive Engine lookup, which no PENDING row prod has ever produced could
        # use. Both are gone. If a lookup helper reappears, this fails and THE
        # REMIT in hsbi_token_snapshot gets re-read before it goes any further.
        for name in ("fetch_issues_to_recipient", "get_history_url", "httpx"):
            self.assertFalse(
                hasattr(issue_module, name),
                f"hivesbi.issue.{name} is back; see THE REMIT before keeping it",
            )

    def test_ambiguous_errors_are_not_treated_as_never_broadcast(self):
        # These may have reached the chain; assuming failure would double-issue.
        for message in (
            "HTTPSConnectionPool: Read timed out",
            "Connection aborted",
            "Unknown error",
            "",
            None,
        ):
            self.assertFalse(never_reached_chain(message), message)


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
        issue_balance_tokens(FakeDB2(self.conn), issuer, "pik")

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
        issue_balance_tokens(FakeDB2(self.conn), issuer, "pik")

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

        issue_balance_tokens(FakeDB2(self.conn), issuer, "pik")

        self.assertEqual(issuer.calls, [])

    def test_abc_pik_uses_its_own_column_and_rationale(self):
        self._insert_holder(T_PIK, abc_pik=Decimal("3.000"))
        issuer = FakeIssuer(trx_id="chain_abc_ok")
        issue_balance_tokens(
            FakeDB2(self.conn), issuer, "Pending Balance Conversion"
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


class WarnStuckPendingTests(DBTestCase):
    """A row that resolution could not settle has to be announced: it blocks
    its (recipient, rationale) in issue_balance_tokens and the symptom is
    silent — a skipped member raises no error, they just stop being paid."""

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


class StuckPendingRecoveryTests(DBTestCase):
    """The block-limit stall: a broadcast rejected with
    "Account hivesbi already submitted 5 custom json operation(s) this block"
    leaves a PENDING row, and that row blocks its member in issue_balance_tokens
    until something proves the broadcast's fate.

    Only one thing does that automatically, and it reads the error already stored
    on the row. A row whose error proves nothing stays PENDING for an operator, so
    these tests fix which errors count as proof and which do not.
    """

    BLOCK_LIMIT_ERROR = (
        "Assert Exception:insert_info.first->second <= HIVE_CUSTOM_OP_BLOCK_LIMIT: "
        "Account hivesbi already submitted 5 custom json operation(s) this block."
    )

    def test_block_limit_error_fails_the_row_immediately(self):
        # The direct fix for the reported stall. The 6th custom_json in a block is
        # asserted away during validation, so it can never appear on chain — there
        # is nothing for reconciliation to discover. Failing the row here lets the
        # next cycle re-issue instead of blocking the member until a chain lookup
        # proves an absence that was certain at broadcast time.
        self.x(
            "INSERT INTO tokenholders (member_name, abc_pik) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE abc_pik = VALUES(abc_pik)",
            (T_HOLDER, Decimal("1.234")),
        )
        issuer = FakeIssuer(fail=True, error=self.BLOCK_LIMIT_ERROR)
        issue_balance_tokens(
            FakeDB2(self.conn), issuer, "Pending Balance Conversion"
        )

        row = self.x(
            "SELECT status, error_message FROM token_issuance_log "
            "WHERE recipient = %s AND rationale = %s ORDER BY id DESC",
            (T_HOLDER, "Pending Balance Conversion"),
        ).fetchone()
        self.assertEqual(row[0], "FAILURE")
        self.assertIn("HIVE_CUSTOM_OP_BLOCK_LIMIT", row[1])
        # Balance untouched, so the retry issues the full amount.
        abc = self.x(
            "SELECT abc_pik FROM tokenholders WHERE member_name = %s", (T_HOLDER,)
        ).fetchone()[0]
        self.assertEqual(Decimal(str(abc)), Decimal("1.234"))

        # Next cycle: no PENDING row blocks the member, so it is attempted again.
        retry_issuer = FakeIssuer(trx_id="chain_retry_ok")
        issue_balance_tokens(
            FakeDB2(self.conn), retry_issuer, "Pending Balance Conversion"
        )
        self.assertEqual(retry_issuer.calls, [(T_HOLDER, 1.234)])
        abc_after = self.x(
            "SELECT abc_pik FROM tokenholders WHERE member_name = %s", (T_HOLDER,)
        ).fetchone()[0]
        self.assertEqual(Decimal(str(abc_after)), Decimal("0.000"))

    def test_ambiguous_broadcast_error_still_stays_pending(self):
        # Only provably-rejected errors may be failed at broadcast time. A timeout
        # may still have landed, and failing it would mint the same tokens twice,
        # so the row is held PENDING for an operator instead.
        log_id = insert_pending_issuance(
            self.conn, T_HOLDER, Decimal("1.234"), "Pending Balance Conversion"
        )
        status = record_broadcast_error(self.conn, log_id, "Read timed out")
        self.assertEqual(status, "PENDING")
        self.assertEqual(
            self.x(
                "SELECT status FROM token_issuance_log WHERE id = %s", (log_id,)
            ).fetchone()[0],
            "PENDING",
        )

    def _pending(self, recipient, units, rationale, issued_at, error=None):
        return self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', %s, %s, %s)
            """,
            (recipient, units, error, rationale, issued_at),
        ).lastrowid

    def test_sweep_fails_stuck_rows_from_their_recorded_error(self):
        # The only automatic resolution there is. record_pending_error already
        # stored the proof of non-inclusion on the row, so a row stuck from before
        # record_broadcast_error existed can be failed by reading its own
        # error_message: no chain scan, no Hive Engine request, no lookback.
        now = datetime.now(timezone.utc)
        stuck_id = self._pending(
            T_HOLDER,
            Decimal("1.234"),
            "Pending Balance Conversion",
            now - timedelta(days=60),
            self.BLOCK_LIMIT_ERROR,
        )
        self.assertEqual(fail_pending_with_proven_non_inclusion(self.conn), 1)

        row = self.x(
            "SELECT status, error_message FROM token_issuance_log WHERE id = %s",
            (stuck_id,),
        ).fetchone()
        self.assertEqual(row[0], "FAILURE")
        # The proof stays on the row as the diagnostic record of why it failed.
        self.assertIn("HIVE_CUSTOM_OP_BLOCK_LIMIT", row[1])

    def test_sweep_leaves_rows_that_carry_no_proof_of_their_fate(self):
        # A timeout may have landed, and a row with no error at all died between
        # the write-ahead insert and either outcome write. Neither proves anything,
        # so both are held for an operator rather than guessed at.
        now = datetime.now(timezone.utc)
        ambiguous_id = self._pending(
            T_HOLDER, Decimal("1.234"), "Pending Balance Conversion",
            now - timedelta(days=60), "HTTPSConnectionPool: Read timed out",
        )
        silent_id = self._pending(
            T_PIK, Decimal("2.500"), "pik", now - timedelta(days=60), None
        )
        self.assertEqual(fail_pending_with_proven_non_inclusion(self.conn), 0)

        statuses = dict(
            self.x(
                "SELECT id, status FROM token_issuance_log WHERE id IN (%s, %s)",
                (ambiguous_id, silent_id),
            ).fetchall()
        )
        self.assertEqual(statuses[ambiguous_id], "PENDING")
        self.assertEqual(statuses[silent_id], "PENDING")

    def test_sweep_does_not_read_marker_underscores_as_sql_wildcards(self):
        # The sweep used to build `error_message LIKE '%HIVE_CUSTOM_OP_BLOCK_LIMIT%'`,
        # where every `_` matches any single character. This message is NOT the
        # marker, so record_broadcast_error would have left it PENDING — but the
        # wildcard pattern matched it. Failing a row whose broadcast did reach the
        # chain re-issues tokens that already exist, minting HSBIDAO twice.
        now = datetime.now(timezone.utc)
        near_miss = self._pending(
            T_HOLDER,
            Decimal("1.234"),
            "Pending Balance Conversion",
            now - timedelta(days=60),
            "Assert Exception: HIVE-CUSTOM-OP-BLOCK-LIMIT was not the reason",
        )
        self.assertFalse(never_reached_chain(
            "Assert Exception: HIVE-CUSTOM-OP-BLOCK-LIMIT was not the reason"
        ))
        self.assertEqual(fail_pending_with_proven_non_inclusion(self.conn), 0)
        self.assertEqual(
            self.x(
                "SELECT status FROM token_issuance_log WHERE id = %s", (near_miss,)
            ).fetchone()[0],
            "PENDING",
        )

class SettledRowShapeTests(DBTestCase):
    """A settled row must not still look like an in-flight one.

    insert_pending_issuance stamps trx_id = 'PENDING' because the transaction id
    is not known yet. Every FAILURE row in prod carries 'N/A' instead, which is
    what "no transaction exists" is spelled as in this table, so the paths that
    settle a row as FAILURE have to replace the placeholder rather than leave it.
    """

    BLOCK_LIMIT_ERROR = StuckPendingRecoveryTests.BLOCK_LIMIT_ERROR

    def _trx_and_status(self, log_id):
        return self.x(
            "SELECT status, trx_id FROM token_issuance_log WHERE id = %s", (log_id,)
        ).fetchone()

    def test_broadcast_time_failure_clears_the_pending_placeholder(self):
        log_id = insert_pending_issuance(
            self.conn, T_HOLDER, Decimal("1.234"), "Pending Balance Conversion"
        )
        self.assertEqual(self._trx_and_status(log_id)[1], "PENDING")
        record_broadcast_error(self.conn, log_id, self.BLOCK_LIMIT_ERROR)
        self.assertEqual(self._trx_and_status(log_id), ("FAILURE", "N/A"))

    def test_sweep_clears_the_pending_placeholder(self):
        log_id = self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', %s, %s, %s)
            """,
            (
                T_HOLDER,
                Decimal("1.234"),
                self.BLOCK_LIMIT_ERROR,
                "Pending Balance Conversion",
                datetime.now(timezone.utc) - timedelta(days=60),
            ),
        ).lastrowid
        self.assertEqual(fail_pending_with_proven_non_inclusion(self.conn), 1)
        self.assertEqual(self._trx_and_status(log_id), ("FAILURE", "N/A"))

    def test_sweep_will_not_overwrite_a_row_that_settled_underneath_it(self):
        """The status guard on the sweep's UPDATE.

        The sweep SELECTs matching PENDING rows and then UPDATEs them by id. The
        SELECT reads a snapshot; the UPDATE reads current committed data. Without
        `AND status = 'PENDING'` a row another connection settled as SUCCESS in
        between would be flipped to FAILURE - and that row's balance is already
        debited, so the member would lose the tokens while the log denied they
        were ever issued.

        The window is simulated by settling the row between the two statements.
        """
        log_id = self.x(
            """
            INSERT INTO token_issuance_log
                (trx_id, recipient, units, status, error_message, rationale, issued_at)
            VALUES ('PENDING', %s, %s, 'PENDING', %s, %s, %s)
            """,
            (
                T_HOLDER,
                Decimal("1.234"),
                self.BLOCK_LIMIT_ERROR,
                "Pending Balance Conversion",
                datetime.now(timezone.utc) - timedelta(days=60),
            ),
        ).lastrowid

        real_conn = self.conn

        class _SettlesAfterTheSelect:
            """Passes everything through, settling the row once the SELECT is done."""

            def __init__(self):
                self.calls = 0

            def exec_driver_sql(self, sql, params=()):
                result = real_conn.exec_driver_sql(sql, params)
                self.calls += 1
                if self.calls == 1:  # the SELECT has read its snapshot
                    real_conn.exec_driver_sql(
                        "UPDATE token_issuance_log "
                        "SET status = 'SUCCESS', trx_id = 'chain_landed_after_all' "
                        "WHERE id = %s",
                        (log_id,),
                    )
                return result

        failed = fail_pending_with_proven_non_inclusion(_SettlesAfterTheSelect())
        self.assertEqual(failed, 0)
        self.assertEqual(
            self.x(
                "SELECT status, trx_id FROM token_issuance_log WHERE id = %s", (log_id,)
            ).fetchone(),
            ("SUCCESS", "chain_landed_after_all"),
        )


class BroadcastThrottleTests(unittest.TestCase):
    """The 5-custom_json-per-block budget belongs to the issuer account, not to
    any one loop, so pacing lives in hivesbi.issue and applies to every path."""

    def _isolate_throttle_state(self, account):
        """Keep fake-clock values out of module state after the test.

        _last_broadcast_started is a module global, so a leftover stamp taken from
        a mocked monotonic clock is compared against the real one by any later
        caller — and on Linux, where time.monotonic() is boot-relative, that can
        be a genuine multi-second sleep inside the test process.
        """
        issue_module._last_broadcast_started.pop(account, None)
        self.addCleanup(issue_module._last_broadcast_started.pop, account, None)

    def test_consecutive_broadcasts_are_spaced_by_the_minimum_interval(self):
        account = "zz_throttle_acct"
        self._isolate_throttle_state(account)
        sleeps = []
        # Each call reads the clock once to measure the gap (except the first,
        # which has nothing to measure against) and once to stamp the broadcast.
        clock = iter([100.0, 100.2, 101.0, 101.1, 102.0])
        with patch.object(issue_module.time, "sleep", side_effect=sleeps.append):
            with patch.object(issue_module.time, "monotonic", lambda: next(clock)):
                issue_module.throttle_broadcast(account)  # first: no wait
                issue_module.throttle_broadcast(account)  # 0.2s on: waits 0.8s
                issue_module.throttle_broadcast(account)  # 0.1s on: waits 0.9s
        # Slow broadcasts pay no extra tax: the round trip counts toward the gap.
        self.assertEqual([round(s, 3) for s in sleeps], [0.8, 0.9])

    def test_first_broadcast_for_an_account_does_not_wait(self):
        account = "zz_throttle_fresh"
        self._isolate_throttle_state(account)
        with patch.object(issue_module.time, "sleep") as slept:
            issue_module.throttle_broadcast(account)
        slept.assert_not_called()

    # --- the throttle is only worth anything if the broadcast paths call it ----
    #
    # The tests above prove throttle_broadcast spaces its own calls. They say
    # nothing about whether anything invokes it, so deleting the call from
    # TokenIssuer.issue left the whole suite green while restoring the stall this
    # work was opened for. These pin the call sites instead.

    def _bare_issuer(self, account, log):
        """A TokenIssuer with __init__ skipped.

        Constructing one for real reads the active key out of the database,
        connects Hive and fetches the account — none of which is what is under
        test here. Only the broadcast methods and the attributes they touch are
        needed, so the object is built directly and the two wallets record the
        order they were called in.
        """
        issuer = issue_module.TokenIssuer.__new__(issue_module.TokenIssuer)
        issuer.account_name = account
        issuer.token_symbol = "HSBIDAO"

        class _Recorder:
            def __init__(self, label):
                self._label = label

            def issue(self, *args, **kwargs):
                log.append(self._label)
                return {"trx_id": "chain_throttle_test"}

            def transfer(self, *args, **kwargs):
                log.append(self._label)
                return {"trx_id": "chain_throttle_test"}

        issuer.engine_wallet = _Recorder("engine")
        issuer.hive_account = _Recorder("base_chain")
        return issuer

    def test_issue_paces_before_it_broadcasts(self):
        account = "zz_throttle_issue"
        log = []
        issuer = self._bare_issuer(account, log)
        with patch.object(
            issue_module, "throttle_broadcast", side_effect=lambda a: log.append(("throttle", a))
        ):
            issuer.issue("zz_recipient", 1.5)
        # Paced, and paced BEFORE the op goes out — throttling afterwards would
        # let the whole loop broadcast into one block and then sleep.
        self.assertEqual(log, [("throttle", account), "engine"])

    def test_engine_transfer_paces_before_it_broadcasts(self):
        account = "zz_throttle_engine_xfer"
        log = []
        issuer = self._bare_issuer(account, log)
        with patch.object(
            issue_module, "throttle_broadcast", side_effect=lambda a: log.append(("throttle", a))
        ):
            issuer.transfer("zz_recipient", 1.5, asset_symbol="HSBIDAO")
        self.assertEqual(log, [("throttle", account), "engine"])

    def test_base_chain_transfer_is_not_paced(self):
        # HIVE/HBD transfers are not custom_json and carry no per-block limit.
        # Pacing them would add a second per refund for nothing, so the routing
        # distinction is pinned here rather than left to a comment.
        account = "zz_throttle_base_xfer"
        log = []
        issuer = self._bare_issuer(account, log)
        with patch.object(
            issue_module, "throttle_broadcast", side_effect=lambda a: log.append(("throttle", a))
        ):
            issuer.transfer("zz_recipient", 0.001, asset_symbol="HIVE")
        self.assertEqual(log, ["base_chain"])

    def test_pacing_survives_a_rebuilt_issuer(self):
        # get_default_token_issuer caches the issuer, and its docstring claims the
        # cache is not what makes pacing work: the budget belongs to the account,
        # so _last_broadcast_started is keyed by name and outlives any one object.
        account = "zz_throttle_rebuilt"
        self._isolate_throttle_state(account)
        log = []
        clock = iter([100.0, 100.2, 101.0])
        with patch.object(issue_module.time, "sleep") as slept:
            with patch.object(issue_module.time, "monotonic", lambda: next(clock)):
                self._bare_issuer(account, log).issue("zz_recipient", 1.0)
                self._bare_issuer(account, log).issue("zz_recipient", 1.0)
        self.assertEqual(log, ["engine", "engine"])
        slept.assert_called_once()
        self.assertAlmostEqual(slept.call_args[0][0], 0.8, places=3)


class DefaultIssuerCacheTests(unittest.TestCase):
    """get_default_token_issuer caches a module-global TokenIssuer.

    hivesbi/parse_hist_op calls it once per Unit Conversion, and building one for
    real re-reads the active key from the database, reconnects Hive and refetches
    the account, so the cache is what keeps that off the per-conversion path.
    These pin the three properties that makes it safe to rely on, and record the
    one it does not have.
    """

    def _isolate_default_issuer(self):
        """Never leak a fake issuer (or a real one) into another test."""
        saved = issue_module._default_issuer
        issue_module._default_issuer = None

        def restore():
            issue_module._default_issuer = saved

        self.addCleanup(restore)

    class _FakeIssuer:
        """Stands in for TokenIssuer so no key read or Hive connection happens."""

        def __init__(self, built):
            built.append(self)
            self.account_name = issue_module.DEFAULT_ISSUER_ACCOUNT
            self.issued = []

        def issue(self, recipient, amount):
            self.issued.append((recipient, amount))
            return {"trx_id": "chain_cached_issuer"}

    def _patch_token_issuer(self, built):
        return patch.object(
            issue_module, "TokenIssuer", lambda *a, **kw: self._FakeIssuer(built)
        )

    def test_issuer_is_constructed_once_and_reused(self):
        # The whole point of the cache: N calls, one key read.
        self._isolate_default_issuer()
        built = []
        with self._patch_token_issuer(built):
            first = issue_module.get_default_token_issuer()
            second = issue_module.get_default_token_issuer()
            third = issue_module.get_default_token_issuer()
        self.assertEqual(len(built), 1)
        self.assertIs(first, second)
        self.assertIs(second, third)

    def test_issue_default_tokens_goes_through_the_cache(self):
        # parse_hist_op's Unit Conversion path calls issue_default_tokens, not
        # get_default_token_issuer directly, so the saving only exists if this
        # entry point shares the cache.
        self._isolate_default_issuer()
        built = []
        with self._patch_token_issuer(built):
            issue_module.issue_default_tokens("zz_cache_recipient", 5)
            issue_module.issue_default_tokens("zz_cache_recipient", 7)
        self.assertEqual(len(built), 1)
        self.assertEqual(built[0].issued, [("zz_cache_recipient", 5), ("zz_cache_recipient", 7)])

    def test_a_failed_construction_is_not_cached(self):
        # If the key read or the Hive connection fails, the global must stay None
        # so the next call retries. Caching the failure would turn one bad moment
        # at startup into every issuance failing for the rest of the run.
        self._isolate_default_issuer()

        def _explode(*args, **kwargs):
            raise RuntimeError("no active key")

        with patch.object(issue_module, "TokenIssuer", _explode):
            with self.assertRaises(RuntimeError):
                issue_module.get_default_token_issuer()
        self.assertIsNone(issue_module._default_issuer)

        built = []
        with self._patch_token_issuer(built):
            recovered = issue_module.get_default_token_issuer()
        self.assertEqual(len(built), 1)
        self.assertIs(recovered, built[0])

    def test_a_cached_issuer_that_starts_failing_is_never_rebuilt(self):
        """Characterisation, not a guarantee: the cache has no invalidation.

        Once built, the same object is handed out for the rest of the process no
        matter how its broadcasts fare. That is the accepted trade — nectar owns
        reconnection and node failover beneath TokenIssuer, so a transport fault
        is not the issuer's to repair — but it does mean a permanently broken
        issuer stays in place. If that is ever observed in prod, this test is the
        place the assumption is written down.
        """
        self._isolate_default_issuer()
        built = []
        with self._patch_token_issuer(built):
            first = issue_module.get_default_token_issuer()

            def _always_fails(recipient, amount):
                raise RuntimeError("connection is gone")

            first.issue = _always_fails
            with self.assertRaises(RuntimeError):
                issue_module.issue_default_tokens("zz_cache_recipient", 1)

            second = issue_module.get_default_token_issuer()
        self.assertIs(second, first)
        self.assertEqual(len(built), 1)


if __name__ == "__main__":
    unittest.main()
