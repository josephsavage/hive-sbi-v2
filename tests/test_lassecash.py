"""Tests for the LasseCash companion vote: hivesbi/magi.py, hivesbi/lassecash.py
and hsbi_lassecash_vote.py.

The pure-logic and client tests run anywhere; they never touch the network. The
DB tests use the local docker MariaDB (`sbi` via config.json databaseConnector2)
inside one transaction that is rolled back. They skip when the database or the
lassecash tables are missing. On an old volume, create the tables from
docker/mariadb/init/01-sbi-schema.sql or reset the volume.

Fixture values come from live Magi reads on 2026-10-23: the post record, gas
figures and the keychain capture of the 1% vote sbi-tokens cast that day.
"""

import json
import unittest
import urllib.error
from datetime import timedelta
from unittest.mock import patch

import hsbi_lassecash_vote as job
from hivesbi import lassecash as lc
from hivesbi.magi import MagiClient, MagiError, qualify

CONTRACT = "vsc1Be4TTjUiHgzhHAfqFn6s3PDAExH2X59fXV"
AUTHOR = "zz_lc_author"
VOTER = "sbi-tokens"

# Captured from Hive Keychain on 2026-10-23 (the custom_json op of the 1% vote).
KEYCHAIN_JSON = (
    '{"net_id":"vsc-mainnet","contract_id":"vsc1Be4TTjUiHgzhHAfqFn6s3PDAExH2X59fXV",'
    '"action":"vote","payload":"hive:lasseehlers|lassecash-runs-on-a-fifth-of-a-household|1",'
    '"rc_limit":7833,"intents":[]}'
)
# Live post_ record of that (deep-window) post.
LIVE_POST_RECORD = "1|110385489|5092313233398|0|0|0|3|2|0|||0"
# Simulated gas: a vote on a registered post, and a first (registering) vote.
GAS_REVOTE = 98_627_858
GAS_REGISTERING = 434_742_987


# --- DB availability ----------------------------------------------------------
_DB_ENGINE = None
_DB_SKIP_REASON = ""
try:
    import dataset
    from hivesbi.settings import Config

    _DB_ENGINE = dataset.connect(Config.load()["databaseConnector2"]).engine
    with _DB_ENGINE.connect() as _probe:
        _tables = {
            row[0]
            for row in _probe.exec_driver_sql("SHOW TABLES LIKE 'lassecash%%'").fetchall()
        }
    if not {"lassecash_stakers", "lassecash_votes"} <= _tables:
        _DB_SKIP_REASON = "lassecash tables missing; apply docker/mariadb/init/01-sbi-schema.sql"
        _DB_ENGINE = None
except Exception as exc:  # pragma: no cover - environment dependent
    _DB_SKIP_REASON = f"sbi database unavailable: {exc}"
    _DB_ENGINE = None


class _SameConnTxn:
    """engine.begin() on the test's own connection, without committing."""

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn

    def begin(self):
        return _SameConnTxn(self._conn)


class FakeDB:
    def __init__(self, conn):
        self.engine = _FakeEngine(conn)


class FakeMagi:
    """Stands in for MagiClient. State lookups and dry runs are recorded."""

    def __init__(self, state=None, sim=None, rc=100_000, verdicts=None):
        self.state = dict(state or {})
        self.sim = sim or {"success": True, "err_msg": "", "gas_used": GAS_REVOTE, "ret": "voted"}
        self.rc = rc
        self.verdicts = verdicts or {}
        self.simulated = []

    def get_state(self, contract_id, keys):
        return {k: self.state[k] for k in keys if k in self.state}

    def simulate_call(self, caller, contract_id, action, payload):
        self.simulated.append(payload)
        return dict(self.sim)

    def available_rc(self, account):
        return self.rc

    def transaction_verdict(self, trx_id):
        return self.verdicts.get(trx_id, (None, None))


class FakeComment(dict):
    def __init__(self, main_post=True, json_metadata=None, **fields):
        super().__init__(**fields)
        self._main_post = main_post
        self.json_metadata = json_metadata

    def is_main_post(self):
        return self._main_post


# --- pure logic -----------------------------------------------------------------
class PayloadTests(unittest.TestCase):
    def test_vote_json_matches_keychain_capture(self):
        body = lc.build_vote_json(
            CONTRACT, "lasseehlers", "lassecash-runs-on-a-fifth-of-a-household", 1, 7833
        )
        self.assertEqual(body, json.loads(KEYCHAIN_JSON))

    def test_author_is_qualified_once(self):
        # An unqualified author aborts on chain ("author needs ... L-Shares"),
        # because the contract reads shr_<author> verbatim.
        self.assertEqual(lc.build_vote_payload("alice", "p", 7), "hive:alice|p|7")
        self.assertEqual(lc.build_vote_payload("hive:alice", "p", 7), "hive:alice|p|7")

    def test_state_keys(self):
        self.assertEqual(lc.shares_key(VOTER), "shr_hive:sbi-tokens")
        self.assertEqual(lc.post_key("alice", "p"), "post_hive:alice_p")
        self.assertEqual(lc.vote_key("alice", "p", VOTER), "pv_hive:alice_p_hive:sbi-tokens")

    def test_post_window(self):
        self.assertEqual(lc.post_window(LIVE_POST_RECORD), lc.DEEP)
        self.assertEqual(lc.post_window("0|1|2|0|0|0"), lc.VIRAL)
        self.assertEqual(lc.post_window(None), lc.VIRAL)
        self.assertEqual(lc.post_window("garbage"), lc.VIRAL)


class WeightTests(unittest.TestCase):
    def test_budgets_match_meter_regeneration(self):
        # 1,000 weight-percent per full meter; viral refills in 7 days, deep in 30.
        self.assertEqual(lc.weight_budget(lc.VIRAL), 4285)
        self.assertEqual(lc.weight_budget(lc.DEEP), 1000)

    def test_weight_is_budget_over_posts_clamped(self):
        self.assertEqual(lc.compute_weight(lc.VIRAL, 0), 100)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 1), 100)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 42), 100)  # 102 -> cap
        self.assertEqual(lc.compute_weight(lc.VIRAL, 43), 99)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 100), 42)
        self.assertEqual(lc.compute_weight(lc.DEEP, 20), 50)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 10_000), 1)  # contract floor


class RcLimitTests(unittest.TestCase):
    def test_three_times_simulated_with_floor_and_ceiling(self):
        self.assertEqual(lc.size_rc_limit(GAS_REVOTE), 987 * 3)
        self.assertEqual(lc.size_rc_limit(GAS_REGISTERING), 4348 * 3)
        self.assertEqual(lc.size_rc_limit(0), lc.RC_FLOOR)
        self.assertEqual(lc.size_rc_limit(10**12), lc.RC_CEILING)


class RefusalTests(unittest.TestCase):
    def test_permanent_refusals(self):
        self.assertTrue(lc.is_permanent_refusal(
            "msg: author needs 100000000000 L-Shares for this post to earn"))
        self.assertTrue(lc.is_permanent_refusal("msg: voting has closed"))
        self.assertFalse(lc.is_permanent_refusal("msg: not enough vote power"))
        self.assertFalse(lc.is_permanent_refusal(""))


class TagTests(unittest.TestCase):
    def test_tag_sources(self):
        self.assertTrue(lc.has_lassecash_tag(FakeComment(tags=["life", "lassecash"])))
        self.assertTrue(lc.has_lassecash_tag(
            FakeComment(json_metadata='{"tags": ["LasseCash"]}')))
        self.assertTrue(lc.has_lassecash_tag(
            FakeComment(json_metadata={"tags": ["x"]}, category="lassecash")))

    def test_untagged_and_lookalike_posts(self):
        self.assertFalse(lc.has_lassecash_tag(FakeComment(tags=["lassecash-news"])))
        self.assertFalse(lc.has_lassecash_tag(FakeComment(json_metadata="not json")))
        self.assertFalse(lc.has_lassecash_tag(FakeComment()))


class SettingsTests(unittest.TestCase):
    def test_missing_block_means_disabled(self):
        settings = lc.LassecashSettings.from_config({})
        self.assertFalse(settings.enabled)
        self.assertFalse(settings.broadcast)
        self.assertEqual(settings.voter, VOTER)

    def test_block_values(self):
        settings = lc.LassecashSettings.from_config(
            {"lassecash": {"enabled": True, "broadcast": True, "threshold_viral": 5}}
        )
        self.assertTrue(settings.enabled and settings.broadcast)
        self.assertEqual(settings.threshold_viral, 5)


# --- Magi client ---------------------------------------------------------------
class FakeTransport:
    """Replaces the HTTP POST. answers maps url -> list of answers or exceptions."""

    def __init__(self, answers):
        self.answers = {url: list(items) for url, items in answers.items()}
        self.calls = []

    def __call__(self, url, body, timeout):
        self.calls.append((url, json.loads(body)))
        item = self.answers[url].pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class MagiClientTests(unittest.TestCase):
    def test_qualify(self):
        self.assertEqual(qualify("alice"), "hive:alice")
        self.assertEqual(qualify("did:key:z6"), "did:key:z6")

    def test_failover_to_next_node(self):
        post = FakeTransport({
            "a": [urllib.error.URLError("refused")],
            "b": [{"data": {"getAccountRC": {"amount": 53992, "max_rcs": 91161}}}],
        })
        client = MagiClient(nodes=["a", "b"], post=post)
        self.assertEqual(client.available_rc(VOTER), 53992)
        self.assertEqual(post.calls[1][1]["variables"], {"a": "hive:sbi-tokens"})

    def test_all_nodes_down_names_every_node(self):
        post = FakeTransport({"a": [OSError("down")], "b": [OSError("down")]})
        with self.assertRaises(MagiError) as ctx:
            MagiClient(nodes=["a", "b"], post=post).available_rc(VOTER)
        self.assertIn("a:", str(ctx.exception))
        self.assertIn("b:", str(ctx.exception))

    def test_graphql_error_does_not_fail_over(self):
        post = FakeTransport({"a": [{"errors": [{"message": "bad key"}]}], "b": []})
        with self.assertRaises(MagiError):
            MagiClient(nodes=["a", "b"], post=post).available_rc(VOTER)
        self.assertEqual(len(post.calls), 1)

    def test_unknown_account_rc_is_none_not_zero(self):
        post = FakeTransport({"a": [{"data": {"getAccountRC": {"amount": 0, "max_rcs": 0}}}]})
        self.assertIsNone(MagiClient(nodes=["a"], post=post).available_rc("nobody"))

    def test_get_state_batches_and_drops_empty_values(self):
        keys = [f"k{i}" for i in range(250)]
        answers = []
        for start in (0, 100, 200):
            batch = keys[start : start + 100]
            values = {k: ("" if k == "k5" else None if k == "k6" else "1") for k in batch}
            answers.append({"data": {"getStateByKeys": values}})
        post = FakeTransport({"a": answers})
        found = MagiClient(nodes=["a"], post=post).get_state(CONTRACT, keys)
        self.assertEqual([len(c[1]["variables"]["k"]) for c in post.calls], [100, 100, 50])
        self.assertEqual(len(found), 248)
        self.assertNotIn("k5", found)
        self.assertNotIn("k6", found)

    def test_simulate_sends_qualified_caller(self):
        post = FakeTransport({"a": [{"data": {"simulateContractCalls": [
            {"success": False, "err_msg": "msg: author needs 1 L-Shares", "gas_used": 125260669}
        ]}}]})
        result = MagiClient(nodes=["a"], post=post).simulate_call(VOTER, CONTRACT, "vote", "x")
        sent = post.calls[0][1]["variables"]["i"]
        self.assertEqual(sent["required_auths"], ["hive:sbi-tokens"])
        self.assertEqual(sent["calls"][0]["rc_limit"], 100_000)
        self.assertFalse(result["success"])
        self.assertEqual(result["gas_used"], 125260669)

    def test_transaction_verdicts(self):
        post = FakeTransport({"a": [
            {"data": {"findTransaction": [{"status": "CONFIRMED", "output": [{"id": "c1"}]}]}},
            {"data": {"findTransaction": [{"status": "FAILED", "output": [{"id": "c2"}]}]}},
            {"data": {"getDagByCID": json.dumps({"results": [{"errMsg": "not enough vote power"}]})}},
            {"data": {"findTransaction": []}},
        ]})
        client = MagiClient(nodes=["a"], post=post)
        self.assertEqual(client.transaction_verdict("t1"), ("CONFIRMED", None))
        self.assertEqual(client.transaction_verdict("t2"), ("FAILED", "not enough vote power"))
        self.assertEqual(client.transaction_verdict("t3"), (None, None))


# --- DB-backed --------------------------------------------------------------------
@unittest.skipUnless(_DB_ENGINE is not None, _DB_SKIP_REASON)
class DBTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = _DB_ENGINE.connect()
        self.trans = self.conn.begin()
        # Start from empty tables; the rollback restores whatever was there.
        self.conn.exec_driver_sql("DELETE FROM lassecash_votes")
        self.conn.exec_driver_sql("DELETE FROM lassecash_stakers")
        self.db = FakeDB(self.conn)
        self.now = lc.utcnow().replace(microsecond=0)
        self.settings = lc.LassecashSettings(enabled=True, broadcast=True)

    def tearDown(self):
        self.trans.rollback()
        self.conn.close()

    def add_vote_row(self, permlink, status=lc.QUALIFIED, post_age=timedelta(hours=1),
                     created_at=None, window=lc.VIRAL, **fields):
        created_at = created_at or self.now
        lc.insert_vote_row(self.conn, f"@{AUTHOR}/{permlink}", AUTHOR, permlink,
                           self.now - post_age, VOTER, status, created_at)
        row_id = self.conn.exec_driver_sql(
            "SELECT id FROM lassecash_votes WHERE permlink = %s", (permlink,)).scalar()
        if window != lc.VIRAL or fields:
            lc.update_vote_row(self.conn, row_id, created_at, vote_window=window, **fields)
        return row_id

    def row(self, row_id):
        result = self.conn.exec_driver_sql(
            f"SELECT {lc._VOTE_COLUMNS} FROM lassecash_votes WHERE id = %s", (row_id,))
        return lc._vote_rows(result)[0]


class StakerSyncTests(DBTestCase):
    def test_snapshot_marks_eligibility_and_skips_until_stale(self):
        threshold = self.settings.threshold_viral
        magi = FakeMagi(state={
            lc.shares_key("zz_lc_big"): str(threshold),
            lc.shares_key("zz_lc_small"): str(threshold - 1),
        })
        members = ["zz_lc_big", "zz_lc_small", "zz_lc_none"]
        eligible = lc.sync_lassecash_stakers(self.db, magi, self.settings, members, 144, self.now)
        self.assertEqual(eligible, 1)
        self.assertEqual(lc.eligible_stakers(self.conn), ["zz_lc_big"])
        count = self.conn.exec_driver_sql("SELECT COUNT(*) FROM lassecash_stakers").scalar()
        self.assertEqual(count, 3)  # zero rows are kept, so the refresh time survives

        later = self.now + timedelta(minutes=143)
        self.assertIsNone(
            lc.sync_lassecash_stakers(self.db, magi, self.settings, members, 144, later))

    def test_snapshot_survives_its_own_cleanup_with_the_real_clock(self):
        # Regression: a clock with microseconds stored rows that read back
        # older than `now`, and the cleanup deleted the whole new snapshot.
        magi = FakeMagi(state={lc.shares_key("zz_lc_big"): str(self.settings.threshold_viral)})
        lc.sync_lassecash_stakers(self.db, magi, self.settings, ["zz_lc_big"], 144)
        self.assertEqual(lc.eligible_stakers(self.conn), ["zz_lc_big"])
        self.assertEqual(lc.utcnow().microsecond, 0)

    def test_refresh_drops_departed_members(self):
        magi = FakeMagi()
        lc.sync_lassecash_stakers(self.db, magi, self.settings, ["zz_lc_a", "zz_lc_b"], 144, self.now)
        later = self.now + timedelta(minutes=145)
        lc.sync_lassecash_stakers(self.db, magi, self.settings, ["zz_lc_a"], 144, later)
        accounts = [r[0] for r in self.conn.exec_driver_sql(
            "SELECT account FROM lassecash_stakers").fetchall()]
        self.assertEqual(accounts, ["zz_lc_a"])


class QualifyTests(DBTestCase):
    def test_tagged_post_qualifies_and_untagged_is_ignored_once(self):
        lc.replace_stakers(self.conn, {AUTHOR: self.settings.threshold_viral},
                           self.settings.threshold_viral, self.now)
        posts = [
            {"authorperm": f"@{AUTHOR}/tagged", "author": AUTHOR, "created": self.now},
            {"authorperm": f"@{AUTHOR}/plain", "author": AUTHOR, "created": self.now},
            {"authorperm": f"@{AUTHOR}/unreachable", "author": AUTHOR, "created": self.now},
        ]
        comments = {
            f"@{AUTHOR}/tagged": FakeComment(tags=["lassecash"]),
            f"@{AUTHOR}/plain": FakeComment(tags=["hive"]),
            f"@{AUTHOR}/unreachable": None,
        }
        loads = []

        def load(authorperm):
            loads.append(authorperm)
            return comments[authorperm]

        with patch.object(job, "recent_voted_posts", return_value=posts):
            self.assertEqual(job.qualify_posts(self.db, self.db, self.settings, load, self.now), 2)
            loads.clear()
            self.assertEqual(job.qualify_posts(self.db, self.db, self.settings, load, self.now), 0)
        self.assertEqual(loads, [f"@{AUTHOR}/unreachable"])  # known rows are not refetched
        statuses = dict(self.conn.exec_driver_sql(
            "SELECT permlink, status FROM lassecash_votes").fetchall())
        self.assertEqual(statuses, {"tagged": lc.QUALIFIED, "plain": lc.IGNORED})

    def test_no_eligible_staker_means_no_post_query(self):
        with patch.object(job, "recent_voted_posts") as query:
            self.assertEqual(job.qualify_posts(self.db, self.db, self.settings, None, self.now), 0)
        query.assert_not_called()


class CastTests(DBTestCase):
    def broadcaster(self, result=None, error=None):
        sent = []

        def broadcast(body):
            sent.append(body)
            if error:
                raise error
            return result if result is not None else {"trx_id": "f" * 40}

        return broadcast, sent

    def test_broadcasts_one_vote_with_sized_rc_limit(self):
        first = self.add_vote_row("first", post_age=timedelta(hours=2))
        second = self.add_vote_row("second")
        magi = FakeMagi()
        broadcast, sent = self.broadcaster()
        self.assertEqual(job.cast_votes(self.db, magi, self.settings, broadcast, self.now), 1)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["payload"], f"hive:{AUTHOR}|first|100")
        self.assertEqual(sent[0]["rc_limit"], 2961)
        row = self.row(first)
        self.assertEqual((row["status"], row["trx_id"], row["weight"]), (lc.BROADCAST, "f" * 40, 100))
        self.assertEqual(self.row(second)["status"], lc.QUALIFIED)  # next run

    def test_weight_spreads_budget_over_lookback_posts(self):
        for i in range(99):
            self.add_vote_row(f"old{i}", status=lc.SUCCESS, created_at=self.now - timedelta(days=5))
        self.add_vote_row("ignored", status=lc.IGNORED)
        self.add_vote_row("too-old", status=lc.SUCCESS, created_at=self.now - timedelta(days=31))
        row_id = self.add_vote_row("current")
        broadcast, sent = self.broadcaster()
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        self.assertEqual(self.row(row_id)["weight"], 4285 // 100)
        self.assertTrue(sent[0]["payload"].endswith("|current|42"))

    def test_deep_post_uses_deep_budget(self):
        row_id = self.add_vote_row("deep")
        magi = FakeMagi(state={lc.post_key(AUTHOR, "deep"): LIVE_POST_RECORD})
        broadcast, _ = self.broadcaster()
        job.cast_votes(self.db, magi, self.settings, broadcast, self.now)
        row = self.row(row_id)
        self.assertEqual((row["vote_window"], row["weight"]), (lc.DEEP, 100))

    def test_existing_vote_is_not_replaced(self):
        row_id = self.add_vote_row("manual")
        magi = FakeMagi(state={
            lc.vote_key(AUTHOR, "manual", VOTER): "1155022280",
            lc.post_key(AUTHOR, "manual"): LIVE_POST_RECORD,
        })
        broadcast, sent = self.broadcaster()
        job.cast_votes(self.db, magi, self.settings, broadcast, self.now)
        self.assertEqual(sent, [])
        self.assertEqual(magi.simulated, [])
        row = self.row(row_id)
        # The skipped row still records its window, so it counts against the
        # deep meter's budget, not the viral one.
        self.assertEqual((row["status"], row["vote_window"]), (lc.SKIPPED, lc.DEEP))

    def test_permanent_refusal_skips_and_transient_defers(self):
        permanent = self.add_vote_row("below-threshold")
        broadcast, sent = self.broadcaster()
        refusal = {"success": False, "gas_used": 0,
                   "err_msg": "msg: author needs 100000000000 L-Shares for this post to earn\nfile: :1:1"}
        job.cast_votes(self.db, FakeMagi(sim=refusal), self.settings, broadcast, self.now)
        row = self.row(permanent)
        self.assertEqual(row["status"], lc.SKIPPED)
        self.assertNotIn("file:", row["error_message"])

        transient = self.add_vote_row("low-power")
        power = {"success": False, "gas_used": 0, "err_msg": "msg: not enough vote power"}
        job.cast_votes(self.db, FakeMagi(sim=power), self.settings, broadcast, self.now)
        row = self.row(transient)
        self.assertEqual((row["status"], row["error_message"]),
                         (lc.QUALIFIED, "msg: not enough vote power"))
        self.assertEqual(sent, [])

    def test_rc_short_defers_without_broadcast(self):
        row_id = self.add_vote_row("rc")
        broadcast, sent = self.broadcaster()
        job.cast_votes(self.db, FakeMagi(rc=1000), self.settings, broadcast, self.now)
        self.assertEqual(sent, [])
        row = self.row(row_id)
        self.assertEqual(row["status"], lc.QUALIFIED)
        self.assertIn("RC short", row["error_message"])

    def test_shadow_run_never_broadcasts(self):
        a = self.add_vote_row("a")
        b = self.add_vote_row("b")
        shadow = lc.LassecashSettings(enabled=True, broadcast=False)
        broadcast, sent = self.broadcaster()
        self.assertEqual(job.cast_votes(self.db, FakeMagi(), shadow, broadcast, self.now), 0)
        self.assertEqual(sent, [])
        for row_id in (a, b):
            row = self.row(row_id)
            self.assertEqual((row["status"], row["weight"]), (lc.QUALIFIED, 100))

    def test_broadcast_errors(self):
        block_limit = self.add_vote_row("block-limit")
        broadcast, _ = self.broadcaster(error=RuntimeError(
            "Assert Exception: HIVE_CUSTOM_OP_BLOCK_LIMIT: already submitted 5"))
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        self.assertEqual(self.row(block_limit)["status"], lc.QUALIFIED)

        self.conn.exec_driver_sql("DELETE FROM lassecash_votes")
        unknown = self.add_vote_row("timeout")
        broadcast, _ = self.broadcaster(error=RuntimeError("read timed out"))
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        self.assertEqual(self.row(unknown)["status"], lc.PENDING)

    def test_candidate_window_expiry(self):
        row_id = self.add_vote_row("stale", post_age=timedelta(days=4),
                                   error_message="msg: not enough vote power")
        broadcast, sent = self.broadcaster()
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        row = self.row(row_id)
        self.assertEqual(row["status"], lc.SKIPPED)
        self.assertEqual(row["error_message"], "candidate window ended: msg: not enough vote power")
        self.assertEqual(sent, [])


class SettleTests(DBTestCase):
    def test_vote_record_proves_success(self):
        row_id = self.add_vote_row("landed", status=lc.PENDING)
        magi = FakeMagi(state={lc.vote_key(AUTHOR, "landed", VOTER): "1155022280"})
        job.settle_votes(self.db, magi, self.settings, self.now)
        self.assertEqual(self.row(row_id)["status"], lc.SUCCESS)

    def test_failed_verdict_records_the_contract_error(self):
        row_id = self.add_vote_row("refused", status=lc.BROADCAST, trx_id="a" * 40)
        magi = FakeMagi(verdicts={"a" * 40: ("FAILED", "not enough vote power")})
        job.settle_votes(self.db, magi, self.settings, self.now)
        row = self.row(row_id)
        self.assertEqual((row["status"], row["error_message"]), (lc.FAILED, "not enough vote power"))

    def test_no_verdict_waits_then_settles(self):
        broadcast = self.add_vote_row("silent", status=lc.BROADCAST, trx_id="b" * 40)
        pending = self.add_vote_row("lost", status=lc.PENDING)
        magi = FakeMagi()
        job.settle_votes(self.db, magi, self.settings, self.now + timedelta(minutes=5))
        self.assertEqual(self.row(broadcast)["status"], lc.BROADCAST)
        self.assertEqual(self.row(pending)["status"], lc.PENDING)

        job.settle_votes(self.db, magi, self.settings, self.now + lc.CONFIRM_TIMEOUT)
        self.assertEqual(self.row(broadcast)["status"], lc.FAILED)
        self.assertEqual(self.row(pending)["status"], lc.QUALIFIED)


if __name__ == "__main__":
    unittest.main()
