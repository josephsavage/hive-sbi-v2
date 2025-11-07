"""Local shim for `nectar.account.Account` used by `hivesbi.memo_parser` in tests.

This minimal Account class only validates account name format and otherwise
does not perform network calls. It raises Exception for invalid account names
so the existing parsing logic in `hivesbi.memo_parser` behaves as expected.
"""
import re


_NAME_RE = re.compile(r"^[a-z0-9\-\.]{3,16}$")


class Account:
    def __init__(self, name, blockchain_instance=None):
        # Basic validation similar to Hive/Steem username rules used by tests
        if name is None:
            raise Exception("invalid account")
        nm = name.strip("'\" ")
        if not _NAME_RE.match(nm):
            raise Exception("not an account")
        self.name = nm

    def __repr__(self):
        return f"<Account {self.name}>"
