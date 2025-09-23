# This Python file uses the following encoding: utf-8
import logging

from nectar.account import Account
from nectar.instance import shared_blockchain_instance
from urllib.parse import urlparse

log = logging.getLogger(__name__)


class MemoParser(object):
    def __init__(self, blockchain_instance=None):
        self.hive = blockchain_instance or shared_blockchain_instance()
        self.allowed_memo_words = [
            "for",
            "and",
            "sponsor",
            "shares",
            "share",
            "sponsorship",
            "please",
            "hive",
            "thanks",
            "additional",
            "sponsee",
            "sponsoring",
            "sponser",
            "one",
            "you",
            "thank",
            "enroll",
            "sponsering:",
            "sponsoring;",
            "sponsoring:",
            "would",
            "like",
            "too",
            "enroll:",
            "sponsor:",
        ]
        self.allowed_hosts = {"steemit.com", "hive.blog", "peakd.com", "ecency.com"}

    def parse_memo(self, memo, shares, account):
        # print(f"DEBUG: parse_memo called with memo='{memo}', shares={shares}, account='{account}'")
        if memo[0] == "'":
            memo = memo[1:]
        if memo[-1] == "'":
            memo = memo[:-1]
        words_memo = memo.strip().lower().replace(",", "  ").replace('"', "").split(" ")
        # print(f"DEBUG: Processed memo words: {words_memo}")

        sponsors = {}
        no_numbers = True
        amount_left = shares
        word_count = 0
        not_parsed_words = []
        n_words = len(words_memo)
        digit_found = None
        sponsor = None
        account_error = False

        for w in words_memo:
            if len(w) == 0:
                continue
            if w in self.allowed_memo_words:
                continue
            if amount_left >= 1:
                account_name = ""
                account_found = False
                w_digit = w.replace("x", "", 1).replace("-", "", 1).replace(";", "", 1)
                if w_digit.isdigit():
                    no_numbers = False
                    digit_found = int(w_digit)
                elif len(w) < 3:
                    continue
                elif w.startswith("http://") or w.startswith("https://"):
                    try:
                        parsed = urlparse(w)
                        host = parsed.netloc.lower()
                        if host.startswith("www."):
                            host = host[4:]
                        allowed_hosts = self.allowed_hosts
                        if host in allowed_hosts:
                            path = parsed.path or ""
                            if path.startswith("/@"):
                                account_name = path[2:]
                                # Allow optional trailing slash but no extra path segments
                                if "/" in account_name:
                                    if (
                                        account_name.endswith("/")
                                        and account_name.count("/") == 1
                                    ):
                                        account_name = account_name[:-1]
                                    else:
                                        raise ValueError(
                                            "URL path contains extra segments"
                                        )
                                account_name = (
                                    account_name.replace("!", "")
                                    .replace('"', "")
                                    .replace(";", "")
                                )
                                if account_name and account_name[0] == "'":
                                    account_name = account_name[1:]
                                if account_name and account_name[-1] == "'":
                                    account_name = account_name[:-1]
                                if account_name and account_name[-1] == ".":
                                    account_name = account_name[:-1]
                                if account_name and account_name[0] == "@":
                                    account_name = account_name[1:]
                                account_name = account_name.strip()
                                _acc = Account(
                                    account_name, blockchain_instance=self.hive
                                )
                                account_found = True
                            else:
                                # Not a profile URL with /@username
                                pass
                        else:
                            # Not a recognized frontend host
                            pass
                    except Exception:
                        # print(f"DEBUG: Exception in frontend URL parsing: {e}")
                        print(account_name + " is not an account")
                        account_error = True
                elif len(w.split(":")) == 2 and "/" not in w:
                    try:
                        account_name1 = w.split(":")[0]
                        account_name = w.split(":")[1]
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]
                        if account_name1[0] == "'":
                            account_name1 = account_name1[1:]
                        if account_name1[-1] == "'":
                            account_name1 = account_name1[:-1]
                        if account_name1[0] == "@":
                            account_name1 = account_name1[1:]
                        if account_name[0] == "@":
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        account_name1 = account_name1.strip()
                        _acc1 = Account(account_name1, blockchain_instance=self.hive)
                        _acc = Account(account_name, blockchain_instance=self.hive)
                        account_found = True
                        if sponsor is None:
                            sponsor = account_name1
                        else:
                            account_error = True
                    except Exception:
                        # print(f"DEBUG: Exception in colon parsing: {e}")
                        print(account_name + " is not an account")
                        account_error = True
                elif w[0] == "@":
                    try:
                        account_name = (
                            w[1:].replace("!", "").replace('"', "").replace(";", "")
                        )
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]
                        if account_name[-1] == ".":
                            account_name = account_name[:-1]
                        if account_name[0] == "@":
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        _acc = Account(account_name, blockchain_instance=self.hive)
                        account_found = True

                    except Exception:
                        # print(f"DEBUG: Exception in @ parsing: {e}")
                        print(account_name + " is not an account")
                        account_error = True
                elif len(w.split("@")) > 1:
                    try:
                        account_name = (
                            w.replace("!", "")
                            .replace('"', "")
                            .replace(";", "")
                            .split("@")[1]
                        )
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]
                        if account_name[-1] == ".":
                            account_name = account_name[:-1]
                        if account_name[0] == "@":
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        _acc = Account(account_name, blockchain_instance=self.hive)
                        account_found = True

                    except Exception:
                        # print(f"DEBUG: Exception in @split parsing: {e}")
                        print(account_name + " is not an account")
                        account_error = True
                elif len(w) > 16:
                    continue

                else:
                    try:
                        account_name = w.replace("!", "").replace('"', "")
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]
                        if account_name[-1] == ".":
                            account_name = account_name[:-1]
                        if account_name[0] == "@":
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        _acc = Account(account_name, blockchain_instance=self.hive)
                        account_found = True
                    except Exception:
                        # print(f"DEBUG: Exception in fallback parsing: {e}")
                        print(account_name + " is not an account")
                        not_parsed_words.append(w)
                        word_count += 1
                        account_error = True
                if account_found and account_name != "" and account_name != account:
                    if digit_found is not None:
                        sponsors[account_name] = digit_found
                        amount_left -= digit_found
                        digit_found = None
                    elif account_name in sponsors:
                        sponsors[account_name] += 1
                        amount_left -= 1
                    else:
                        sponsors[account_name] = 1
                        amount_left -= 1
        if n_words == 1 and len(sponsors) == 0:
            try:
                account_name = (
                    words_memo[0]
                    .replace(",", " ")
                    .replace("!", " ")
                    .replace('"', "")
                    .replace("/", " ")
                )
                if account_name:
                    if account_name[0] == "'":
                        account_name = account_name[1:]
                    if account_name and account_name[-1] == "'":
                        account_name = account_name[:-1]
                    if account_name and account_name[-1] == ".":
                        account_name = account_name[:-1]
                    if account_name and account_name[0] == "@":
                        account_name = account_name[1:]
                    account_name = account_name.strip()
                    Account(account_name, blockchain_instance=self.hive)
                    if account_name != account:
                        sponsors[account_name] = 1
                        amount_left -= 1
                else:
                    # Empty account_name, skip
                    pass
            except Exception:
                account_error = True
                # print(f"DEBUG: Exception in single word parsing: {e}")
                print(account_name + " is not an account")
        if len(sponsors) == 1 and shares > 1 and no_numbers:
            for a in sponsors:
                sponsors[a] = shares
        elif (
            len(sponsors) == 1
            and shares > 1
            and not no_numbers
            and digit_found is not None
        ):
            for a in sponsors:
                sponsors[a] = digit_found
        elif len(sponsors) > 0 and shares % len(sponsors) == 0 and no_numbers:
            for a in sponsors:
                sponsors[a] = shares // len(sponsors)
        if sponsor is None:
            sponsor = account
        if account_error and len(sponsors) == shares:
            account_error = False
        # print(f"DEBUG: parse_memo returning sponsor='{sponsor}', sponsors={sponsors}, not_parsed_words={not_parsed_words}, account_error={account_error}")
        return sponsor, sponsors, not_parsed_words, account_error
