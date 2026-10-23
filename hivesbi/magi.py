"""Read-only client for the Magi (formerly VSC) layer-2 GraphQL API.

The client reads contract state, resource credits (RC), transaction verdicts
and contract dry runs. It never signs or broadcasts anything: a contract call
reaches Magi as a Hive custom_json, which the caller broadcasts through nectar.

Every query goes to the first node that answers. The node list and its order
come from the LasseCash client (api/src/magi-nodes.ts), which fails over the
same way. On 2026-10-23 api.vsc.eco refused connections while both other nodes
answered, so a single hard-coded node is not enough.
"""

import json
import urllib.error
import urllib.request

DEFAULT_MAGI_NODES = (
    "https://api.okinoko.io/api/v1/graphql",
    "https://vsc.techcoderx.com/api/v1/graphql",
    "https://api.vsc.eco/api/v1/graphql",
)

# getStateByKeys refuses a request outside 1..100 keys.
MAX_STATE_KEYS = 100

# A dry run reserves nothing, so it probes at the node's maximum. A lower probe
# can report gas_limit_hit for a call the real limit would carry.
SIMULATION_RC_LIMIT = 100_000

# vsc.techcoderx.com answers 403 to urllib's default "Python-urllib/3.x"
# User-Agent (checked 2026-10-23), so every request names itself.
USER_AGENT = "hive-sbi/1.0 (+https://hivesbi.com)"


class MagiError(Exception):
    """Every node failed, or a node answered with GraphQL errors."""


def qualify(account):
    """Return the Magi address of a Hive account: "hive:<account>".

    Magi keys balances, shares and RC by the qualified address. getAccountRC
    answers 0/0 for a bare name instead of an error, and a contract reads a
    different state key, so an unqualified name fails silently.
    """
    return account if ":" in account else "hive:" + account


def _http_post(url, body, timeout):
    request = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


class MagiClient:
    def __init__(self, nodes=DEFAULT_MAGI_NODES, timeout=20, post=_http_post):
        if not nodes:
            raise ValueError("at least one Magi node is required")
        self.nodes = list(nodes)
        self.timeout = timeout
        self._post = post

    def query(self, query, variables=None):
        """Run one GraphQL query and return its `data` object.

        A transport failure moves on to the next node. A GraphQL error does
        not: every node runs the same code, so the next node gives the same
        answer, and the error names the real fault.
        """
        body = json.dumps({"query": query, "variables": variables or {}})
        failures = []
        for url in self.nodes:
            try:
                answer = self._post(url, body, self.timeout)
            except (urllib.error.URLError, OSError, ValueError) as exc:
                failures.append(f"{url}: {exc}")
                continue
            if answer.get("errors"):
                messages = "; ".join(
                    str(e.get("message", e)) for e in answer["errors"]
                )
                raise MagiError(f"{url}: GraphQL error: {messages}")
            if answer.get("data") is None:
                failures.append(f"{url}: no data in answer")
                continue
            return answer["data"]
        raise MagiError("all Magi nodes failed: " + " | ".join(failures))

    def get_state(self, contract_id, keys):
        """Return {key: value} for the keys that hold a value.

        Magi reads a missing key as null or as an empty string. Both mean
        absent here, so a caller tests membership and never sees "".
        """
        found = {}
        keys = list(dict.fromkeys(keys))
        for start in range(0, len(keys), MAX_STATE_KEYS):
            batch = keys[start : start + MAX_STATE_KEYS]
            data = self.query(
                "query($c: String!, $k: [String!]!) "
                "{ getStateByKeys(contractId: $c, keys: $k) }",
                {"c": contract_id, "k": batch},
            )
            for key, value in (data.get("getStateByKeys") or {}).items():
                if value not in (None, ""):
                    found[key] = value
        return found

    def available_rc(self, account):
        """Return the account's available RC, or None when the node cannot tell.

        A max_rcs of 0 is the node's answer for an address it does not know.
        That zero is not a reading, so it maps to None, not to "no RC".
        """
        data = self.query(
            "query($a: String!) { getAccountRC(account: $a) { amount max_rcs } }",
            {"a": qualify(account)},
        )
        record = data.get("getAccountRC")
        if not record or not record.get("max_rcs"):
            return None
        return int(record["amount"])

    def simulate_call(self, caller, contract_id, action, payload):
        """Dry-run one contract call as `caller`. Nothing is broadcast or reserved.

        Returns {"success", "err_msg", "gas_used", "ret"}. The dry run executes
        the contract's own checks, so a refusal names the exact rule that
        refused the call.
        """
        data = self.query(
            "query($i: SimulateContractCallsInput!) { simulateContractCalls(input: $i) "
            "{ success err_msg gas_used ret } }",
            {
                "i": {
                    "tx_id": "sim",
                    "required_auths": [qualify(caller)],
                    "calls": [
                        {
                            "contract_id": contract_id,
                            "action": action,
                            "payload": payload,
                            "rc_limit": SIMULATION_RC_LIMIT,
                            "intents": [],
                        }
                    ],
                }
            },
        )
        rows = data.get("simulateContractCalls") or []
        if not rows:
            raise MagiError("simulateContractCalls returned no result")
        row = rows[-1]
        return {
            "success": bool(row.get("success")),
            "err_msg": row.get("err_msg") or "",
            "gas_used": int(row.get("gas_used") or 0),
            "ret": row.get("ret"),
        }

    def transaction_verdict(self, trx_id):
        """Return (status, error) for a call that arrived in a Hive transaction.

        Magi indexes such a call under the 40-hex Hive transaction id. status
        is CONFIRMED, FAILED, or None while Magi has no verdict yet. The
        refusal text is not on the transaction record; it is in the output
        DAG, so a FAILED verdict costs one more read.
        """
        data = self.query(
            "query($id: String!) { findTransaction(filterOptions: {byId: $id}) "
            "{ status output { id } } }",
            {"id": trx_id},
        )
        records = data.get("findTransaction") or []
        if not records:
            return None, None
        status = records[0].get("status")
        if status == "CONFIRMED":
            return "CONFIRMED", None
        if status != "FAILED":
            return None, None
        outputs = records[0].get("output") or []
        if not outputs:
            return "FAILED", None
        try:
            dag = self.query(
                "query($c: String!) { getDagByCID(cidString: $c) }",
                {"c": outputs[0]["id"]},
            )
            results = json.loads(dag["getDagByCID"]).get("results") or [{}]
            return "FAILED", results[0].get("errMsg") or results[0].get("err")
        except (MagiError, KeyError, TypeError, ValueError):
            return "FAILED", None
