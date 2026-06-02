import json
import unittest
from decimal import Decimal

from hivesbi.storage import TrxDB
from hsbi_check_delegation import calculate_virtual_tokens
from hsbi_token_snapshot import (
    calculate_management_issue_amount,
    _parse_engine_issue,
    reconcile_recent_issuances,
)


class FakeTable:
    def __init__(self, rows):
        self.rows = rows
        self.updated = []

    def find(self, **filters):
        for row in self.rows:
            if all(row.get(key) == value for key, value in filters.items()):
                yield row

    def update(self, data, keys):
        self.updated.append((data, keys))


class FakeDB:
    def __init__(self, rows):
        self.table = FakeTable(rows)

    def __getitem__(self, name):
        if name != "trx":
            raise KeyError(name)
        return self.table


class FakeConn:
    def __init__(self):
        self.inserted = []

    def exec_driver_sql(self, sql, params=None):
        if sql.strip().upper().startswith("SELECT"):
            return self
        self.inserted.append((sql, params))
        return self

    def fetchone(self):
        return None


class VirtualTokenTests(unittest.TestCase):
    def test_virtual_tokens_are_decimal_hp_times_two(self):
        self.assertEqual(calculate_virtual_tokens("0.001"), Decimal("0.002"))
        self.assertEqual(calculate_virtual_tokens("123.4567"), Decimal("246.913"))

    def test_clear_delegation_accrual_zeroes_shares_and_vests(self):
        db = FakeDB(
            [
                {
                    "index": 1,
                    "source": "steembasicincome",
                    "account": "alice",
                    "status": "Valid",
                    "share_type": "Delegation",
                    "shares": 10,
                    "vests": Decimal("20.000000"),
                }
            ]
        )

        TrxDB(db).clear_delegation_accrual("steembasicincome", "alice")

        self.assertEqual(
            db.table.updated,
            [
                (
                    {
                        "index": 1,
                        "source": "steembasicincome",
                        "shares": 0,
                        "vests": 0,
                    },
                    ["index", "source"],
                )
            ],
        )

    def test_management_issue_formula_floors_to_three_decimals(self):
        self.assertEqual(
            calculate_management_issue_amount(Decimal("1000.000"), Decimal("0")),
            Decimal("111.111"),
        )
        self.assertEqual(
            calculate_management_issue_amount(Decimal("1000.000"), Decimal("100.000")),
            Decimal("0.000"),
        )

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
            {
                "trx_id": "abc123",
                "recipient": "josephsavage",
                "units": Decimal("12.345"),
            },
        )

    def test_reconciliation_does_not_infer_management_from_recipient(self):
        conn = FakeConn()
        issue = {
            "trx_id": "abc123",
            "recipient": "josephsavage",
            "units": Decimal("12.345"),
        }

        with unittest.mock.patch(
            "hsbi_token_snapshot.fetch_recent_chain_issuances",
            return_value=[issue],
        ):
            reconcile_recent_issuances(conn, object())

        self.assertEqual(conn.inserted[0][1][-1], "reconciled")


if __name__ == "__main__":
    unittest.main()
