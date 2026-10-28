"""Tests for the LasseCash companion vote: hivesbi/magi.py, hivesbi/lassecash.py
and hsbi_lassecash_vote.py.

The pure-logic, client and entry-point tests run anywhere; they never touch the
network. The DB tests use the local docker MariaDB (`sbi` via config.json
databaseConnector2) inside one transaction that is rolled back. They skip when
the database or the lassecash tables are missing, or when lassecash_votes lacks
the current columns. On an old volume, recreate the tables from
docker/mariadb/init/01-sbi-schema.sql or reset the volume. The member_root_posts
test reads posts_comments in `sbi_steem_ops` (databaseConnector) the same way,
and skips when that database is unavailable.

Fixture values come from live Magi reads on 2026-10-23: the post record, gas
figures and the keychain capture of the 1% vote sbi-tokens cast that day.
"""

import contextlib
import http.client
import io
import json
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import hsbi_lassecash_vote as job
from hivesbi import lassecash as lc
from hivesbi.magi import MagiClient, MagiError, qualify

CONTRACT = "vsc1Be4TTjUiHgzhHAfqFn6s3PDAExH2X59fXV"
AUTHOR = "zz_lc_author"
VOTER = "sbi-tokens"
SBI_VOTER = "zz_lc_sbi"

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
        _columns = set()
        if "lassecash_votes" in _tables:
            _columns = {
                row[0]
                for row in _probe.exec_driver_sql("SHOW COLUMNS FROM lassecash_votes").fetchall()
            }
    if not {"lassecash_stakers", "lassecash_votes"} <= _tables:
        _DB_SKIP_REASON = "lassecash tables missing; apply docker/mariadb/init/01-sbi-schema.sql"
        _DB_ENGINE = None
    elif not {"hive_voted", "broadcast_at"} <= _columns:
        _DB_SKIP_REASON = "lassecash_votes is outdated; recreate it from docker/mariadb/init/01-sbi-schema.sql"
        _DB_ENGINE = None
except Exception as exc:  # pragma: no cover - environment dependent
    _DB_SKIP_REASON = f"sbi database unavailable: {exc}"
    _DB_ENGINE = None

_OPS_ENGINE = None
_OPS_SKIP_REASON = ""
try:
    import dataset
    from hivesbi.settings import Config

    _OPS_ENGINE = dataset.connect(Config.load()["databaseConnector"]).engine
    with _OPS_ENGINE.connect() as _probe:
        if not _probe.exec_driver_sql("SHOW TABLES LIKE 'posts_comments'").fetchall():
            _OPS_SKIP_REASON = "posts_comments missing; apply docker/mariadb/init/02-sbi-steem-ops-schema.sql"
            _OPS_ENGINE = None
except Exception as exc:  # pragma: no cover - environment dependent
    _OPS_SKIP_REASON = f"sbi_steem_ops database unavailable: {exc}"
    _OPS_ENGINE = None


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
    """Stands in for MagiClient. State lookups and dry runs are recorded.

    A dry run whose payload contains one of sim_errors raises MagiError, the
    way a GraphQL error does.
    """

    def __init__(self, state=None, sim=None, rc=100_000, verdicts=None, calls=None,
                 sim_errors=()):
        self.state = dict(state or {})
        self.sim = sim or {"success": True, "err_msg": "", "gas_used": GAS_REVOTE, "ret": "voted"}
        self.rc = rc
        self.verdicts = verdicts or {}
        self.calls = list(calls or [])
        self.sim_errors = tuple(sim_errors)
        self.simulated = []
        self.from_block = None

    def get_state(self, contract_id, keys):
        return {k: self.state[k] for k in keys if k in self.state}

    def simulate_call(self, caller, contract_id, action, payload):
        self.simulated.append(payload)
        if any(marker in payload for marker in self.sim_errors):
            raise MagiError("node: GraphQL error: simulated")
        return dict(self.sim)

    def available_rc(self, account):
        return self.rc

    def transaction_verdict(self, trx_id):
        return self.verdicts.get(trx_id, (None, None))

    def contract_vote_calls(self, contract_id, from_block):
        self.from_block = from_block
        return list(self.calls)


class FakeComment(dict):
    def __init__(self, main_post=True, json_metadata=None, votes=(), **fields):
        super().__init__(**fields)
        self._main_post = main_post
        self.json_metadata = json_metadata
        self._votes = [{"voter": voter} for voter in votes]

    def is_main_post(self):
        return self._main_post

    def get_votes(self):
        return list(self._votes)


def member_post(permlink, created, voted=True, skip=False, author=AUTHOR):
    """One posts_comments row as member_root_posts returns it."""
    return {"authorperm": f"@{author}/{permlink}", "author": author, "created": created,
            "voted": voted, "skip": skip}


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


class VoteOpTests(unittest.TestCase):
    """is_vote_op reads a nectar account-history op (shape checked on 2026-10-23)."""

    def op(self, **fields):
        base = {"id": "vsc.call", "required_auths": [], "required_posting_auths": [VOTER],
                "json": KEYCHAIN_JSON, "trx_id": "6448aa65545a140395e61cb95addfd36de1704a2"}
        base.update(fields)
        return base

    def test_matches_the_voter_call_on_the_post(self):
        permlink = "lassecash-runs-on-a-fifth-of-a-household"
        self.assertTrue(lc.is_vote_op(self.op(), CONTRACT, VOTER, "lasseehlers", permlink))
        self.assertTrue(lc.is_vote_op(self.op(required_posting_auths=[], required_auths=[VOTER]),
                                      CONTRACT, VOTER, "lasseehlers", permlink))

    def test_rejects_other_posts_contracts_voters_and_ids(self):
        permlink = "lassecash-runs-on-a-fifth-of-a-household"
        # A permlink that is a prefix of the voted one is another post.
        self.assertFalse(lc.is_vote_op(self.op(), CONTRACT, VOTER, "lasseehlers", "lassecash-runs"))
        self.assertFalse(lc.is_vote_op(self.op(), "vsc1Other", VOTER, "lasseehlers", permlink))
        self.assertFalse(lc.is_vote_op(self.op(), CONTRACT, "someone", "lasseehlers", permlink))
        self.assertFalse(lc.is_vote_op(self.op(id="other"), CONTRACT, VOTER, "lasseehlers", permlink))
        self.assertFalse(lc.is_vote_op(self.op(json="not json"), CONTRACT, VOTER, "lasseehlers", permlink))


class FindBroadcastTests(unittest.TestCase):
    """find_broadcast proves a call absent only from a history that covers the transaction."""

    attempted = datetime(2026, 10, 24, 15, 0)
    vote_trx = "6448aa65545a140395e61cb95addfd36de1704a2"

    def op(self, minutes, vote=False):
        """A custom_json op `minutes` after the attempt, in nectar's history shape."""
        at = self.attempted + timedelta(minutes=minutes)
        return {"id": "vsc.call", "required_auths": [], "required_posting_auths": [VOTER],
                "json": KEYCHAIN_JSON if vote else '{"action": "claim_pool", "payload": "1"}',
                "trx_id": self.vote_trx if vote else "0" * 40,
                "timestamp": at.strftime("%Y-%m-%dT%H:%M:%S")}

    def find(self, ops, head_minutes=30, blocks_behind=0):
        """Run find_broadcast on a node whose head is `head_minutes` after the attempt."""
        hv = Mock()
        head = self.attempted + timedelta(minutes=head_minutes)
        hv.get_dynamic_global_properties.return_value = {
            "time": head.strftime("%Y-%m-%dT%H:%M:%S"),
            "head_block_number": 1_000 + blocks_behind,
            "last_irreversible_block_num": 1_000,
        }
        account = Mock()
        account.history_reverse.return_value = iter(ops)
        row = {"author": "lasseehlers", "permlink": "lassecash-runs-on-a-fifth-of-a-household",
               "broadcast_at": self.attempted, "updated_at": self.attempted}
        self.account_factory = Mock(return_value=account)
        with patch.object(job, "Account", self.account_factory):
            return job.find_broadcast(hv, lc.LassecashSettings(), row)

    def test_finds_the_vote_call(self):
        self.assertEqual(self.find([self.op(20), self.op(1, vote=True), self.op(-20)]), self.vote_trx)

    def test_history_past_the_search_start_proves_absence(self):
        # The search starts BROADCAST_SEARCH_MARGIN (10 min) before the attempt.
        self.assertIsNone(self.find([self.op(20), self.op(-5), self.op(-11)]))

    def test_empty_history_proves_nothing(self):
        # nectar walks an empty or null history answer as an account without operations.
        with self.assertRaises(job.HistoryNotCovered):
            self.find([])

    def test_history_that_ends_inside_the_search_window_proves_nothing(self):
        with self.assertRaises(job.HistoryNotCovered):
            self.find([self.op(20), self.op(-5)])

    def test_lagging_node_proves_nothing(self):
        # Expiration (5 min) plus the margin (10 min): irreversible blocks must pass +15 min.
        with self.assertRaises(job.HistoryNotCovered):
            self.find([self.op(-11)], head_minutes=15)
        self.account_factory.assert_not_called()
        # Head at +16 min, 20 blocks (60 s) ahead of the last irreversible block: +15 min.
        with self.assertRaises(job.HistoryNotCovered):
            self.find([self.op(-11)], head_minutes=16, blocks_behind=20)
        self.assertIsNone(self.find([self.op(-11)], head_minutes=16))


class WeightTests(unittest.TestCase):
    def test_budgets_match_meter_regeneration(self):
        # 1,000 weight-percent per full meter; viral refills in 7 days, deep in 30.
        self.assertEqual(lc.weight_budget(lc.VIRAL), 4285)
        self.assertEqual(lc.weight_budget(lc.DEEP), 1000)

    def test_weight_is_budget_over_votes_clamped(self):
        self.assertEqual(lc.compute_weight(lc.VIRAL, 0), 100)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 1), 100)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 42), 100)  # 102 -> cap
        self.assertEqual(lc.compute_weight(lc.VIRAL, 43), 99)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 100), 42)
        self.assertEqual(lc.compute_weight(lc.DEEP, 20), 50)
        self.assertEqual(lc.compute_weight(lc.VIRAL, 10_000), 1)  # contract floor


class CapTests(unittest.TestCase):
    t0 = datetime(2026, 10, 1, 12, 0)

    def post(self, name, created, hive_voted, author=AUTHOR):
        return {"authorperm": name, "author": author, "created": created, "hive_voted": hive_voted}

    def test_hive_voted_posts_bypass_the_cap(self):
        posts = [self.post("a", self.t0, True), self.post("b", self.t0 + timedelta(hours=1), True)]
        counted, capped = lc.apply_cap(posts)
        self.assertEqual([p["authorperm"] for p in counted], ["a", "b"])
        self.assertEqual(capped, [])

    def test_posts_without_hive_vote_need_24h_since_the_last_counted_vote(self):
        posts = [
            self.post("hive", self.t0, True),
            self.post("plain1", self.t0 + timedelta(hours=2), False),  # votes at +26 h
            self.post("plain2", self.t0 + timedelta(hours=3), False),  # votes at +27 h: capped
            self.post("other", self.t0 + timedelta(hours=3), False, author="zz_lc_other"),
        ]
        counted, capped = lc.apply_cap(posts)
        self.assertEqual(sorted(p["authorperm"] for p in counted), ["hive", "other", "plain1"])
        self.assertEqual([p["authorperm"] for p in capped], ["plain2"])

    def test_vote_time_is_creation_or_window_close(self):
        self.assertEqual(lc.baseline_vote_time(self.post("a", self.t0, True)), self.t0)
        self.assertEqual(lc.baseline_vote_time(self.post("a", self.t0, False)),
                         self.t0 + lc.HIVE_VOTE_WINDOW)

    def test_settle_timeout_outlasts_the_transaction_expiration(self):
        # A PENDING row is proved missing only after its transaction expired.
        self.assertGreater(lc.CONFIRM_TIMEOUT, timedelta(seconds=lc.TX_EXPIRATION_S))


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

    def test_sbi_voted_reads_the_post_votes(self):
        self.assertTrue(lc.sbi_voted(FakeComment(votes=["x", SBI_VOTER]), [SBI_VOTER]))
        self.assertFalse(lc.sbi_voted(FakeComment(votes=["x"]), [SBI_VOTER]))


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

    def test_disabled_refresh_touches_nothing(self):
        # The refresh swallows every error, so a None result does not prove
        # the guard. The member storage, the DB and Magi must stay untouched.
        for cfg in ({}, {"lassecash": {"enabled": False}}):
            member, db2, magi = Mock(), Mock(), Mock()
            rt = {"cfg": cfg, "db2": db2, "storages": {"member": member}}
            self.assertIsNone(lc.refresh_lassecash_stakers(rt, 144, magi=magi))
            self.assertEqual((member.mock_calls, db2.mock_calls, magi.mock_calls), ([], [], []))


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

    def test_failover_sticks_to_the_node_that_answered(self):
        # A hanging first node costs one timeout per client, not one per query.
        rc = {"data": {"getAccountRC": {"amount": 1, "max_rcs": 2}}}
        post = FakeTransport({"a": [OSError("timed out")], "b": [rc, rc]})
        client = MagiClient(nodes=["a", "b"], post=post)
        client.available_rc(VOTER)
        client.available_rc(VOTER)
        self.assertEqual([url for url, _ in post.calls], ["a", "b", "b"])
        self.assertEqual(client.nodes, ["b", "a"])

    def test_all_nodes_down_names_every_node(self):
        post = FakeTransport({"a": [OSError("down")], "b": [OSError("down")]})
        with self.assertRaises(MagiError) as ctx:
            MagiClient(nodes=["a", "b"], post=post).available_rc(VOTER)
        self.assertIn("a:", str(ctx.exception))
        self.assertIn("b:", str(ctx.exception))

    def test_truncated_or_malformed_answer_fails_over(self):
        # A truncated body raises http.client.IncompleteRead, which is not an
        # OSError. A JSON answer that is not an object has no "errors" key.
        rc = {"data": {"getAccountRC": {"amount": 7, "max_rcs": 9}}}
        for bad in (http.client.IncompleteRead(b"{", 499), [], None):
            post = FakeTransport({"a": [bad], "b": [rc]})
            client = MagiClient(nodes=["a", "b"], post=post)
            self.assertEqual(client.available_rc(VOTER), 7)
            self.assertEqual(client.nodes, ["b", "a"])

    def test_graphql_error_without_data_fails_over(self):
        # A node that lacks a schema field answers errors and no data.
        rc = {"data": {"getAccountRC": {"amount": 5, "max_rcs": 9}}}
        schema_error = [{"message": 'Cannot query field "output"'}]
        for bad in ({"errors": schema_error}, {"errors": schema_error, "data": None}):
            post = FakeTransport({"a": [bad], "b": [rc]})
            client = MagiClient(nodes=["a", "b"], post=post)
            self.assertEqual(client.available_rc(VOTER), 5)
            self.assertEqual(client.nodes, ["b", "a"])

    def test_graphql_error_on_every_node_names_each_error(self):
        post = FakeTransport({"a": [{"errors": [{"message": "bad key"}]}],
                              "b": [{"errors": ["malformed"]}]})
        with self.assertRaises(MagiError) as ctx:
            MagiClient(nodes=["a", "b"], post=post).available_rc(VOTER)
        self.assertIn("a: GraphQL error: bad key", str(ctx.exception))
        self.assertIn("b: GraphQL error: malformed", str(ctx.exception))

    def test_graphql_error_with_data_does_not_fail_over(self):
        answer = {"errors": [{"message": "bad key"}], "data": {"getAccountRC": None}}
        post = FakeTransport({"a": [answer], "b": []})
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

    def test_simulated_success_without_gas_is_an_error(self):
        # rc_limit is sized from gas_used. A success without it would size the
        # limit at RC_FLOOR, below what a registering vote needs.
        for gas in (None, 0):
            post = FakeTransport({"a": [{"data": {"simulateContractCalls": [
                {"success": True, "err_msg": "", "gas_used": gas, "ret": "voted"}
            ]}}]})
            with self.assertRaises(MagiError):
                MagiClient(nodes=["a"], post=post).simulate_call(VOTER, CONTRACT, "vote", "x")

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

    def test_contract_vote_calls_keeps_votes_and_pages(self):
        def record(trx, action, payload):
            return {"id": trx, "anchr_ts": "2026-10-23T16:53:33", "status": "CONFIRMED",
                    "ops": [{"type": "call", "data": {"action": action, "contract_id": CONTRACT,
                                                      "payload": payload}}]}
        post = FakeTransport({"a": [
            {"data": {"findTransaction": [record("t1", "vote", "hive:a|p1|5"),
                                          record("t2", "claim_pool", "1")]}},
            {"data": {"findTransaction": [record("t3", "vote", "hive:b|p2|100")]}},
        ]})
        calls = MagiClient(nodes=["a"], post=post).contract_vote_calls(CONTRACT, 500, page_size=2)
        self.assertEqual([(c["trx_id"], c["author"], c["permlink"], c["weight"]) for c in calls],
                         [("t1", "hive:a", "p1", "5"), ("t3", "hive:b", "p2", "100")])
        self.assertEqual([c[1]["variables"]["o"] for c in post.calls], [0, 2])
        self.assertEqual(post.calls[0][1]["variables"]["f"], 500)


# --- entry points ---------------------------------------------------------------
class RunTests(unittest.TestCase):
    """hsbi_lassecash_vote.run(), the entry point sbirunner.sh calls."""

    def runtime(self, block=None, key_row=None):
        cfg = {} if block is None else {"lassecash": block}
        return {"cfg": cfg, "db": Mock(), "db2": Mock(), "accounts": [SBI_VOTER],
                "storages": {"keys": Mock(get=Mock(return_value=key_row)),
                             "member": Mock(get_all_accounts=Mock(return_value=["zz_lc_a"]))}}

    def run_job(self, rt, argv=(), settle_error=None):
        hv = Mock()
        hv.get_dynamic_global_properties.return_value = {"head_block_number": 100}
        mocks = {}
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(job, "get_runtime", return_value=rt))
            stack.enter_context(patch.object(job, "MagiClient"))
            mocks["make_hive"] = stack.enter_context(patch.object(job, "make_hive", return_value=hv))
            for name in ("settle_votes", "alert_stuck_votes", "qualify_posts", "cast_votes",
                         "sync_lassecash_stakers", "backfill_baseline", "print_backfill_report"):
                mocks[name] = stack.enter_context(patch.object(job, name))
            if settle_error:
                mocks["settle_votes"].side_effect = settle_error
            job.run(list(argv))
        return hv, mocks

    def test_disabled_job_does_not_connect(self):
        _, mocks = self.run_job(self.runtime())
        mocks["make_hive"].assert_not_called()
        mocks["cast_votes"].assert_not_called()

    def test_shadow_run_loads_no_key_and_never_signs(self):
        rt = self.runtime({"enabled": True, "broadcast": False})
        _, mocks = self.run_job(rt)
        kwargs = mocks["make_hive"].call_args.kwargs
        self.assertEqual((kwargs["keys"], kwargs["nobroadcast"], kwargs["expiration"]),
                         ([], True, lc.TX_EXPIRATION_S))
        rt["storages"]["keys"].get.assert_not_called()
        for name in ("settle_votes", "alert_stuck_votes", "qualify_posts", "cast_votes"):
            mocks[name].assert_called_once()

    def test_broadcast_run_signs_custom_json_with_the_posting_key(self):
        rt = self.runtime({"enabled": True, "broadcast": True}, key_row={"wif": "test-wif\r\n"})
        hv, mocks = self.run_job(rt)
        rt["storages"]["keys"].get.assert_called_once_with(VOTER, "posting")
        kwargs = mocks["make_hive"].call_args.kwargs
        self.assertEqual((kwargs["keys"], kwargs["nobroadcast"]), (["test-wif"], False))
        broadcast_vote = mocks["cast_votes"].call_args.args[3]
        body = lc.build_vote_json(CONTRACT, AUTHOR, "p", 5, 2961)
        broadcast_vote(body)
        hv.custom_json.assert_called_once_with("vsc.call", body, required_posting_auths=[VOTER])

    def test_missing_key_stops_before_connecting(self):
        _, mocks = self.run_job(self.runtime({"enabled": True, "broadcast": True}, key_row=None))
        mocks["make_hive"].assert_not_called()

    def test_failed_phase_does_not_skip_the_next(self):
        rt = self.runtime({"enabled": True})
        _, mocks = self.run_job(rt, settle_error=MagiError("all Magi nodes failed"))
        for name in ("alert_stuck_votes", "qualify_posts", "cast_votes"):
            mocks[name].assert_called_once()

    def test_backfill_refreshes_stakers_and_never_signs(self):
        rt = self.runtime({"enabled": True, "broadcast": True}, key_row={"wif": "test-wif"})
        _, mocks = self.run_job(rt, argv=["--backfill"])
        kwargs = mocks["make_hive"].call_args.kwargs
        self.assertEqual((kwargs["keys"], kwargs["nobroadcast"]), ([], True))
        rt["storages"]["keys"].get.assert_not_called()
        self.assertEqual(mocks["sync_lassecash_stakers"].call_args.args[4], 0)
        self.assertEqual(mocks["backfill_baseline"].call_args.args[6], 100)
        mocks["cast_votes"].assert_not_called()


class TokenSnapshotHookTests(unittest.TestCase):
    """hsbi_token_snapshot.main() refreshes the staker list last, inside its cycle gate."""

    def test_refresh_runs_after_issuance(self):
        import hsbi_token_snapshot as snap

        conf = {"share_cycle_min": 144.0, "mana_pct_target": 0, "mana_threshold": 0,
                "last_cycle": datetime(2020, 1, 1, tzinfo=timezone.utc)}
        result = Mock(fetchone=Mock(return_value=Mock(max_mana_pct=0)))
        conn = Mock(exec_driver_sql=Mock(return_value=result))
        db2 = Mock()
        db2.engine.begin.return_value.__enter__ = Mock(return_value=conn)
        db2.engine.begin.return_value.__exit__ = Mock(return_value=False)
        rt = {"storages": {"conf": Mock(get=Mock(return_value=conf))}, "db2": db2}
        calls = Mock()
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(snap, "get_runtime", return_value=rt))
            for name, attr in (("get_default_token_issuer", "issuer"),
                               ("reconcile_recent_issuances", "reconcile"),
                               ("issue_balance_tokens", "balance"),
                               ("sync_tokenholders", "sync"),
                               ("issue_management_tokens", "management"),
                               ("refresh_lassecash_stakers", "refresh")):
                stack.enter_context(patch.object(snap, name, getattr(calls, attr)))
            snap.main()
        self.assertEqual([c[0] for c in calls.mock_calls],
                         ["issuer", "reconcile", "balance", "balance", "sync", "management", "refresh"])
        calls.refresh.assert_called_once_with(rt, 144.0)


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
                     created_at=None, window=lc.VIRAL, hive_voted=True, author=AUTHOR, **fields):
        created_at = created_at or self.now
        authorperm = f"@{author}/{permlink}"
        lc.insert_vote_row(self.conn, authorperm, author, permlink, self.now - post_age, VOTER,
                           status, created_at, hive_voted=hive_voted, vote_window=window)
        row_id = self.conn.exec_driver_sql(
            "SELECT id FROM lassecash_votes WHERE authorperm = %s", (authorperm,)).scalar()
        if fields:
            lc.update_vote_row(self.conn, row_id, created_at, **fields)
        return row_id

    def row(self, row_id):
        result = self.conn.exec_driver_sql(
            f"SELECT {lc._VOTE_COLUMNS} FROM lassecash_votes WHERE id = %s", (row_id,))
        return lc._vote_rows(result)[0]

    def statuses(self):
        return dict(self.conn.exec_driver_sql(
            "SELECT permlink, status FROM lassecash_votes").fetchall())

    def make_staker(self, account=AUTHOR):
        lc.replace_stakers(self.conn, {account: self.settings.threshold_viral},
                           self.settings.threshold_viral, self.now)


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

    def test_refresh_entry_point_never_raises(self):
        class Down(FakeMagi):
            def get_state(self, contract_id, keys):
                raise MagiError("all Magi nodes failed")

        rt = {"cfg": {"lassecash": {"enabled": True}}, "db2": self.db,
              "storages": {"member": Mock(get_all_accounts=Mock(return_value=["zz_lc_a"]))}}
        self.assertIsNone(lc.refresh_lassecash_stakers(rt, 144, magi=Down()))
        self.assertEqual(self.conn.exec_driver_sql("SELECT COUNT(*) FROM lassecash_stakers").scalar(), 0)


class QualifyTests(DBTestCase):
    def qualify(self, posts, load, sbi_voters=(), blacklisted=None):
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(job, "member_root_posts", return_value=posts))
            if blacklisted is not None:
                stack.enter_context(patch.object(job, "blacklisted_members", return_value=blacklisted))
            return job.qualify_posts(self.db, self.db, self.settings, load, list(sbi_voters), self.now)

    def test_tagged_post_qualifies_and_untagged_is_ignored_once(self):
        self.make_staker()
        posts = [member_post(p, self.now) for p in ("tagged", "plain", "unreachable")]
        comments = {
            f"@{AUTHOR}/tagged": FakeComment(tags=["lassecash"]),
            f"@{AUTHOR}/plain": FakeComment(tags=["hive"]),
            f"@{AUTHOR}/unreachable": None,
        }
        loads = []

        def load(authorperm):
            loads.append(authorperm)
            return comments[authorperm]

        self.assertEqual(self.qualify(posts, load), 2)
        loads.clear()
        self.assertEqual(self.qualify(posts, load), 0)
        self.assertEqual(loads, [f"@{AUTHOR}/unreachable"])  # known rows are not refetched
        self.assertEqual(self.statuses(), {"tagged": lc.QUALIFIED, "plain": lc.IGNORED})

    def test_posts_without_hive_vote_wait_for_the_window(self):
        self.make_staker()
        self.make_staker("zz_lc_banned")
        posts = [
            member_post("voted-young", self.now - timedelta(hours=1)),
            member_post("plain-young", self.now - timedelta(hours=2), voted=False),
            member_post("plain-old", self.now - timedelta(hours=25), voted=False),
            member_post("pooled", self.now - timedelta(hours=26), voted=False),
            member_post("blacklist-skip", self.now - timedelta(hours=30), voted=False, skip=True),
            member_post("banned", self.now - timedelta(hours=27), voted=False, author="zz_lc_banned"),
        ]
        loads = []

        def load(authorperm):
            loads.append(authorperm)
            votes = [SBI_VOTER] if authorperm.endswith("/pooled") else []
            return FakeComment(tags=["lassecash"], votes=votes)

        self.assertEqual(self.qualify(posts, load, [SBI_VOTER], {"zz_lc_banned"}), 4)
        self.assertEqual(sorted(loads), sorted(f"@{AUTHOR}/{p}" for p in ("voted-young", "plain-old", "pooled")))
        rows = {r[0]: (r[1], r[2]) for r in self.conn.exec_driver_sql(
            "SELECT permlink, status, hive_voted FROM lassecash_votes").fetchall()}
        self.assertEqual(rows, {
            "voted-young": (lc.QUALIFIED, 1),
            "plain-old": (lc.QUALIFIED, 0),
            "pooled": (lc.QUALIFIED, 1),  # SBI voted, but posts_comments.voted stayed 0
            "banned": (lc.IGNORED, 0),
        })

    def test_permlinks_that_differ_only_in_case_are_two_posts(self):
        # Hive permlinks are case-sensitive, and so is uq_authorperm.
        self.make_staker()
        posts = [member_post("My-Post", self.now - timedelta(hours=2)),
                 member_post("my-post", self.now - timedelta(hours=1))]
        load = lambda authorperm: FakeComment(tags=["lassecash"])  # noqa: E731
        self.assertEqual(self.qualify(posts, load), 2)
        self.assertEqual(self.qualify(posts, load), 0)
        self.assertEqual(self.statuses(), {"My-Post": lc.QUALIFIED, "my-post": lc.QUALIFIED})

    def test_one_failing_post_does_not_stop_the_rest(self):
        self.make_staker()
        posts = [member_post("broken", self.now - timedelta(hours=2)),
                 member_post("fine", self.now - timedelta(hours=1))]

        def load(authorperm):
            if authorperm.endswith("/broken"):
                raise RuntimeError("node answered garbage")
            return FakeComment(tags=["lassecash"])

        self.assertEqual(self.qualify(posts, load), 1)
        self.assertEqual(self.statuses(), {"fine": lc.QUALIFIED})

    def test_no_eligible_staker_means_no_post_query(self):
        with patch.object(job, "member_root_posts") as query:
            self.assertEqual(job.qualify_posts(self.db, self.db, self.settings, None, [], self.now), 0)
        query.assert_not_called()


class CastTests(DBTestCase):
    def setUp(self):
        super().setUp()
        # The write-ahead reads the clock itself; pin it to the run's time.
        clock = patch.object(job, "utcnow", return_value=self.now)
        clock.start()
        self.addCleanup(clock.stop)

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
        self.assertEqual((row["status"], row["trx_id"], row["weight"], row["broadcast_at"]),
                         (lc.BROADCAST, "f" * 40, 100, self.now))
        self.assertEqual(self.row(second)["status"], lc.QUALIFIED)  # next run

    def test_hive_voted_post_goes_ahead_of_older_posts_without_it(self):
        older = self.add_vote_row("plain", post_age=timedelta(hours=30), hive_voted=False)
        newer = self.add_vote_row("hive", post_age=timedelta(hours=1), hive_voted=True)
        broadcast, sent = self.broadcaster()
        self.assertEqual(job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now), 1)
        self.assertEqual([s["payload"] for s in sent], [f"hive:{AUTHOR}|hive|100"])
        self.assertEqual((self.row(newer)["status"], self.row(older)["status"]),
                         (lc.BROADCAST, lc.QUALIFIED))  # the older post waits for the next run

    def test_unsettled_vote_blocks_casting(self):
        # The dry run would check a meter that lacks the unsettled vote. A
        # PENDING vote may have landed, so it blocks the same as a BROADCAST one.
        cases = ((lc.BROADCAST, {"trx_id": "e" * 40}), (lc.PENDING, {}))
        for status, fields in cases:
            with self.subTest(status=status):
                self.conn.exec_driver_sql("DELETE FROM lassecash_votes")
                self.add_vote_row("in-flight", status=status, broadcast_at=self.now, **fields)
                waiting = self.add_vote_row("waiting")
                magi = FakeMagi()
                broadcast, sent = self.broadcaster()
                self.assertEqual(job.cast_votes(self.db, magi, self.settings, broadcast, self.now), 0)
                self.assertEqual((sent, magi.simulated), ([], []))
                self.assertEqual(self.row(waiting)["status"], lc.QUALIFIED)

    def test_weight_spreads_budget_over_lookback_votes(self):
        five_days_ago = self.now - timedelta(days=5)
        for i in range(89):
            self.add_vote_row(f"old{i}", status=lc.SUCCESS, created_at=five_days_ago,
                              broadcast_at=five_days_ago)
        for i in range(10):
            self.add_vote_row(f"base{i}", status=lc.BASELINE, created_at=self.now - timedelta(days=10))
        # Not charged to the meter: an IGNORED row, a FAILED vote, a vote and a
        # BASELINE row from before the lookback, and a DEEP-window vote.
        self.add_vote_row("ignored", status=lc.IGNORED)
        self.add_vote_row("failed", status=lc.FAILED, broadcast_at=five_days_ago)
        old = self.now - timedelta(days=31)
        self.add_vote_row("too-old", status=lc.SUCCESS, created_at=old, broadcast_at=old)
        self.add_vote_row("base-old", status=lc.BASELINE, created_at=old)
        self.add_vote_row("deep", status=lc.SUCCESS, window=lc.DEEP, broadcast_at=five_days_ago)
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

    def test_cap_defers_a_post_without_hive_vote(self):
        self.add_vote_row("earlier", status=lc.SUCCESS, broadcast_at=self.now - timedelta(hours=2))
        row_id = self.add_vote_row("plain", hive_voted=False)
        broadcast, sent = self.broadcaster()
        self.assertEqual(job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now), 0)
        row = self.row(row_id)
        self.assertEqual(sent, [])
        self.assertEqual(row["status"], lc.QUALIFIED)
        self.assertTrue(row["error_message"].startswith("L-cash cap: last vote attempt"))

    def test_cap_allows_a_post_without_hive_vote_after_24h(self):
        self.add_vote_row("earlier", status=lc.SUCCESS, broadcast_at=self.now - timedelta(hours=25))
        row_id = self.add_vote_row("plain", hive_voted=False)
        broadcast, sent = self.broadcaster()
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        self.assertEqual(len(sent), 1)
        self.assertEqual(self.row(row_id)["status"], lc.BROADCAST)

    def test_hive_voted_post_bypasses_the_cap(self):
        self.add_vote_row("earlier", status=lc.SUCCESS, broadcast_at=self.now - timedelta(hours=1))
        row_id = self.add_vote_row("hive", hive_voted=True)
        broadcast, sent = self.broadcaster()
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        self.assertEqual(len(sent), 1)
        self.assertEqual(self.row(row_id)["status"], lc.BROADCAST)

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
        # The skipped row still records its window.
        self.assertEqual((row["status"], row["vote_window"]), (lc.SKIPPED, lc.DEEP))

    def test_magi_error_on_one_row_does_not_stop_the_next(self):
        first = self.add_vote_row("bad", post_age=timedelta(hours=2))
        second = self.add_vote_row("good")
        magi = FakeMagi(sim_errors=("|bad|",))
        broadcast, sent = self.broadcaster()
        self.assertEqual(job.cast_votes(self.db, magi, self.settings, broadcast, self.now), 1)
        self.assertEqual([s["payload"] for s in sent], [f"hive:{AUTHOR}|good|100"])
        row = self.row(first)
        self.assertEqual(row["status"], lc.QUALIFIED)
        self.assertTrue(row["error_message"].startswith("Magi error:"))
        self.assertEqual(self.row(second)["status"], lc.BROADCAST)

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
            self.assertEqual((row["status"], row["weight"], row["broadcast_at"]),
                             (lc.QUALIFIED, 100, None))

    def test_broadcast_errors(self):
        # A provable rejection is no attempt: the row returns to the queue and
        # the author's cap does not start.
        block_limit = self.add_vote_row("block-limit")
        broadcast, _ = self.broadcaster(error=RuntimeError(
            "Assert Exception: HIVE_CUSTOM_OP_BLOCK_LIMIT: already submitted 5"))
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        row = self.row(block_limit)
        self.assertEqual((row["status"], row["broadcast_at"]), (lc.QUALIFIED, None))

        self.conn.exec_driver_sql("DELETE FROM lassecash_votes")
        unknown = self.add_vote_row("timeout")
        broadcast, _ = self.broadcaster(error=RuntimeError("read timed out"))
        job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        row = self.row(unknown)
        self.assertEqual((row["status"], row["broadcast_at"]), (lc.PENDING, self.now))

    def test_broadcast_at_is_the_write_ahead_time(self):
        # find_broadcast dates the transaction from broadcast_at, so the dry
        # runs before the write-ahead must not age it.
        row_id = self.add_vote_row("slow")
        later = self.now + timedelta(minutes=12)
        broadcast, _ = self.broadcaster(error=RuntimeError("read timed out"))
        with patch.object(job, "utcnow", return_value=later):
            job.cast_votes(self.db, FakeMagi(), self.settings, broadcast, self.now)
        row = self.row(row_id)
        self.assertEqual((row["status"], row["broadcast_at"], row["updated_at"]),
                         (lc.PENDING, later, later))

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
    def finder(self, result=None, error=None):
        asked = []

        def find(row):
            asked.append(row["permlink"])
            if error:
                raise error
            return result

        return find, asked

    def test_vote_record_proves_success(self):
        row_id = self.add_vote_row("landed", status=lc.PENDING, broadcast_at=self.now)
        magi = FakeMagi(state={lc.vote_key(AUTHOR, "landed", VOTER): "1155022280"})
        find, asked = self.finder()
        job.settle_votes(self.db, magi, self.settings, find, self.now)
        self.assertEqual(self.row(row_id)["status"], lc.SUCCESS)
        self.assertEqual(asked, [])

    def test_failed_verdict_records_the_contract_error(self):
        row_id = self.add_vote_row("refused", status=lc.BROADCAST, trx_id="a" * 40)
        magi = FakeMagi(verdicts={"a" * 40: ("FAILED", "not enough vote power")})
        job.settle_votes(self.db, magi, self.settings, self.finder()[0], self.now)
        row = self.row(row_id)
        self.assertEqual((row["status"], row["error_message"]), (lc.FAILED, "not enough vote power"))

    def test_broadcast_without_verdict_fails_after_timeout(self):
        row_id = self.add_vote_row("silent", status=lc.BROADCAST, trx_id="b" * 40)
        find = self.finder()[0]
        job.settle_votes(self.db, FakeMagi(), self.settings, find, self.now + timedelta(minutes=5))
        self.assertEqual(self.row(row_id)["status"], lc.BROADCAST)
        job.settle_votes(self.db, FakeMagi(), self.settings, find, self.now + lc.CONFIRM_TIMEOUT)
        self.assertEqual(self.row(row_id)["status"], lc.FAILED)

    def test_verdict_error_still_reaches_the_timeout(self):
        class ErrMagi(FakeMagi):
            def transaction_verdict(self, trx_id):
                raise MagiError('node: GraphQL error: Cannot query field "output"')

        row_id = self.add_vote_row("unanswered", status=lc.BROADCAST, trx_id="d" * 40)
        find = self.finder()[0]
        job.settle_votes(self.db, ErrMagi(), self.settings, find, self.now + timedelta(minutes=5))
        self.assertEqual(self.row(row_id)["status"], lc.BROADCAST)
        job.settle_votes(self.db, ErrMagi(), self.settings, find, self.now + lc.CONFIRM_TIMEOUT)
        row = self.row(row_id)
        self.assertEqual(row["status"], lc.FAILED)
        self.assertIn("verdict lookup failed", row["error_message"])

    def test_pending_found_in_account_history_becomes_broadcast(self):
        row_id = self.add_vote_row("ambiguous", status=lc.PENDING, broadcast_at=self.now)
        find, asked = self.finder(result="c" * 40)
        job.settle_votes(self.db, FakeMagi(), self.settings, find, self.now + timedelta(minutes=5))
        self.assertEqual((self.row(row_id)["status"], asked), (lc.PENDING, []))
        job.settle_votes(self.db, FakeMagi(), self.settings, find, self.now + lc.CONFIRM_TIMEOUT)
        row = self.row(row_id)
        self.assertEqual((row["status"], row["trx_id"], asked), (lc.BROADCAST, "c" * 40, ["ambiguous"]))

    def test_pending_absent_from_account_history_returns_to_the_queue(self):
        row_id = self.add_vote_row("lost", status=lc.PENDING, broadcast_at=self.now)
        find, _ = self.finder(result=None)
        job.settle_votes(self.db, FakeMagi(), self.settings, find, self.now + lc.CONFIRM_TIMEOUT)
        row = self.row(row_id)
        self.assertEqual((row["status"], row["broadcast_at"]), (lc.QUALIFIED, None))

    def test_unproven_absence_stays_pending(self):
        row_id = self.add_vote_row("unproven", status=lc.PENDING, broadcast_at=self.now)
        find, asked = self.finder(error=job.HistoryNotCovered("account history ends after ..."))
        job.settle_votes(self.db, FakeMagi(), self.settings, find, self.now + lc.CONFIRM_TIMEOUT)
        row = self.row(row_id)
        self.assertEqual((row["status"], row["broadcast_at"], asked),
                         (lc.PENDING, self.now, ["unproven"]))

    def test_pending_lookup_error_stays_pending_and_others_settle(self):
        pending = self.add_vote_row("unknown", status=lc.PENDING, broadcast_at=self.now)
        landed = self.add_vote_row("landed", status=lc.BROADCAST, trx_id="a" * 40)
        magi = FakeMagi(state={lc.vote_key(AUTHOR, "landed", VOTER): "1"})
        find, _ = self.finder(error=RuntimeError("account history unavailable"))
        job.settle_votes(self.db, magi, self.settings, find, self.now + lc.CONFIRM_TIMEOUT)
        self.assertEqual(self.row(pending)["status"], lc.PENDING)
        self.assertEqual(self.row(landed)["status"], lc.SUCCESS)

    def test_stuck_pending_vote_prints_an_alert(self):
        # No vote goes out while a row is PENDING, so a row that account
        # history never settles needs an operator.
        stuck = self.add_vote_row("stuck", status=lc.PENDING,
                                  broadcast_at=self.now - lc.STUCK_VOTE_AGE,
                                  error_message="read timed out")
        self.add_vote_row("recent", status=lc.PENDING, broadcast_at=self.now - timedelta(hours=1))
        self.add_vote_row("in-flight", status=lc.BROADCAST, trx_id="a" * 40,
                          broadcast_at=self.now - timedelta(days=1))
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(job.alert_stuck_votes(self.db, self.now), 1)
        self.assertEqual(out.getvalue().splitlines(), [
            f"ALERT: stuck PENDING L-cash vote id={stuck} authorperm=@{AUTHOR}/stuck "
            f"broadcast_at={self.now - lc.STUCK_VOTE_AGE} error=read timed out"
        ])


@unittest.skipUnless(_OPS_ENGINE is not None, _OPS_SKIP_REASON)
class MemberRootPostsTests(unittest.TestCase):
    def setUp(self):
        self.conn = _OPS_ENGINE.connect()
        self.trans = self.conn.begin()
        self.now = lc.utcnow()

    def tearDown(self):
        self.trans.rollback()
        self.conn.close()

    def add_post(self, permlink, created, author=AUTHOR, main_post=1, voted=0, skip=0):
        self.conn.exec_driver_sql(
            "INSERT INTO posts_comments (authorperm, author, created, main_post, voted, skip) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (f"@{author}/{permlink}", author, created, main_post, voted, skip),
        )

    def test_root_posts_by_authors_between_since_and_until(self):
        # The backfill stops where the live job's candidate window starts: a
        # post created at `until` belongs to the live job, not the backfill.
        since = self.now - timedelta(days=lc.LOOKBACK_DAYS)
        until = self.now - timedelta(days=lc.CANDIDATE_DAYS)
        self.add_post("before", since - timedelta(seconds=1))
        self.add_post("at-since", since, voted=1)
        self.add_post("inside", since + timedelta(days=10), skip=1)
        self.add_post("comment", since + timedelta(days=11), main_post=0)
        self.add_post("other", since + timedelta(days=12), author="zz_lc_other")
        self.add_post("at-until", until)
        self.add_post("recent", self.now - timedelta(hours=1))

        posts = lc.member_root_posts(self.conn, [AUTHOR], since, until)
        self.assertEqual(posts, [
            {"authorperm": f"@{AUTHOR}/at-since", "author": AUTHOR, "created": since,
             "voted": True, "skip": False},
            {"authorperm": f"@{AUTHOR}/inside", "author": AUTHOR,
             "created": since + timedelta(days=10), "voted": False, "skip": True},
        ])
        live = lc.member_root_posts(self.conn, [AUTHOR], since)
        self.assertEqual([p["authorperm"] for p in live],
                         [f"@{AUTHOR}/{p}" for p in ("at-since", "inside", "at-until", "recent")])


class BackfillTests(DBTestCase):
    def test_backfill_replays_the_cap_and_reports_registrations(self):
        self.make_staker()
        t0 = self.now - timedelta(days=10)
        posts = [
            member_post("hive", t0),                                         # counts at t0
            member_post("plain1", t0 + timedelta(hours=2), voted=False),     # counts at t0+26 h
            member_post("plain2", t0 + timedelta(hours=3), voted=False),     # t0+27 h: capped
            member_post("untagged", t0 + timedelta(days=2)),
            member_post("fresh", t0 + timedelta(days=5)),
        ]

        def load(authorperm):
            tags = ["hive"] if authorperm.endswith("/untagged") else ["lassecash"]
            return FakeComment(tags=tags)

        calls = [
            {"author": f"hive:{AUTHOR}", "permlink": "hive", "status": "CONFIRMED"},
            {"author": f"hive:{AUTHOR}", "permlink": "untagged", "status": "CONFIRMED"},
            {"author": f"hive:{AUTHOR}", "permlink": "fresh", "status": "FAILED"},
        ]
        magi = FakeMagi(calls=calls)
        with patch.object(job, "member_root_posts", return_value=posts):
            report = job.backfill_baseline(self.db, self.db, magi, self.settings, load,
                                           [SBI_VOTER], 10_000_000, self.now)
            again = job.backfill_baseline(self.db, self.db, magi, self.settings, load,
                                          [SBI_VOTER], 10_000_000, self.now)

        self.assertEqual(magi.from_block, 10_000_000 - 30 * lc.BLOCKS_PER_DAY)
        self.assertEqual(self.statuses(), {"hive": lc.BASELINE, "plain1": lc.BASELINE,
                                           "plain2": lc.SKIPPED, "fresh": lc.BASELINE})
        created = dict(self.conn.exec_driver_sql(
            "SELECT permlink, created_at FROM lassecash_votes").fetchall())
        self.assertEqual(created["hive"], t0)
        self.assertEqual(created["plain1"], t0 + timedelta(hours=26))
        self.assertEqual((report["tagged"], report["baseline"], report["capped"], report["untagged"]),
                         (4, 3, 1, 1))
        self.assertEqual(report["tagged_registered"], [(f"hive:{AUTHOR}", "hive")])
        self.assertEqual(report["untagged_registered"], [(f"hive:{AUTHOR}", "untagged")])
        self.assertIn((f"hive:{AUTHOR}", "fresh"), report["tagged_not_registered"])
        self.assertEqual(again["baseline"], 0)  # a second backfill adds nothing twice

        # BASELINE rows are charged to the meter but never voted.
        since = self.now - timedelta(days=lc.LOOKBACK_DAYS)
        self.assertEqual(lc.meter_votes(self.conn, lc.VIRAL, since), 3)
        self.assertEqual(lc.vote_rows_with_status(self.conn, (lc.QUALIFIED,)), [])


if __name__ == "__main__":
    unittest.main()
