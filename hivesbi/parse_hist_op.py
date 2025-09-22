# This Python file uses the following encoding: utf-8

import json
import logging
from datetime import datetime

from nectar.account import Account
from nectar.amount import Amount
from nectar.instance import shared_blockchain_instance
from nectar.memo import Memo
from nectar.utils import (
    addTzInfo,
    formatTimeString,
)

from hivesbi.memo_parser import MemoParser

log = logging.getLogger(__name__)


class ParseAccountHist(list):
    def __init__(
        self,
        account,
        path,
        trxStorage,
        transactionStorage,
        transactionOutStorage,
        member_data,
        memberStorage=None,
        blockchain_instance=None,
        auditStorage=None,
        rshares_per_hbd=1,
    ):
        self.hive = blockchain_instance or shared_blockchain_instance()
        self.account = Account(account, blockchain_instance=self.hive)
        self.delegated_vests_in = {}
        self.delegated_vests_out = {}
        self.timestamp = addTzInfo(datetime(1970, 1, 1, 0, 0, 0, 0))
        self.path = path
        self.member_data = member_data
        self.memberStorage = memberStorage
        self.memo_parser = MemoParser(blockchain_instance=self.hive)
        self.auditStorage = auditStorage
        self.rshares_per_hbd = rshares_per_hbd
        self.excluded_accounts = [
            "minnowbooster",
            "smartsteem",
            "randowhale",
            "steemvoter",
            "jerrybanfield",
            "boomerang",
            "postpromoter",
            "appreciator",
            "buildawhale",
            "upme",
            "smartmarket",
            "minnowhelper",
            "pushup",
            "steembasicincome",
            "sbi2",
            "sbi3",
            "sbi4",
            "sbi5",
            "sbi6",
            "sbi7",
            "sbi8",
            "sbi9",
        ]

        self.trxStorage = trxStorage
        self.transactionStorage = transactionStorage
        self.transactionOutStorage = transactionOutStorage

    def get_highest_avg_share_age_account(self):
        max_avg_share_age = 0
        account_name = None
        for m in self.member_data:
            self.member_data[m].calc_share_age()
        for m in self.member_data:
            if max_avg_share_age < self.member_data[m]["avg_share_age"]:
                max_avg_share_age = self.member_data[m]["avg_share_age"]
                account_name = m

        return account_name

    def update_delegation(self, op, delegated_in=None, delegated_out=None):
        """Updates the internal state arrays

        :param datetime timestamp: datetime of the update
        :param Amount/float own: vests
        :param dict delegated_in: Incoming delegation
        :param dict delegated_out: Outgoing delegation
        :param Amount/float hive: hive
        :param Amount/float hbd: hbd

        """

        self.timestamp = op["timestamp"]

        new_deleg = dict(self.delegated_vests_in)
        if delegated_in is not None and delegated_in:
            if delegated_in["amount"] == 0 and delegated_in["account"] in new_deleg:
                self.new_delegation_record(
                    op["index"],
                    delegated_in["account"],
                    delegated_in["amount"],
                    op["timestamp"],
                    share_type="RemovedDelegation",
                )
                del new_deleg[delegated_in["account"]]
            elif delegated_in["amount"] > 0:
                self.new_delegation_record(
                    op["index"],
                    delegated_in["account"],
                    delegated_in["amount"],
                    op["timestamp"],
                    share_type="Delegation",
                )
                new_deleg[delegated_in["account"]] = delegated_in["amount"]
            else:
                self.new_delegation_record(
                    op["index"],
                    delegated_in["account"],
                    delegated_in["amount"],
                    op["timestamp"],
                    share_type="RemovedDelegation",
                )
        self.delegated_vests_in = new_deleg

        new_deleg = dict(self.delegated_vests_out)
        if delegated_out is not None and delegated_out:
            if delegated_out["account"] is None:
                # return_vesting_delegation
                for delegatee in new_deleg:
                    if new_deleg[delegatee]["amount"] == delegated_out["amount"]:
                        del new_deleg[delegatee]
                        break

            elif delegated_out["amount"] != 0:
                # new or updated non-zero delegation
                new_deleg[delegated_out["account"]] = delegated_out["amount"]

                # skip undelegations here, wait for 'return_vesting_delegation'
                # del new_deleg[delegated_out['account']]

        self.delegated_vests_out = new_deleg

        delegated_hp_in = {}
        for acc in self.delegated_vests_in:
            vests = Amount(self.delegated_vests_in[acc])
            delegated_hp_in[acc] = str(self.hive.vests_to_hp(vests))
        delegated_hp_out = {}
        for acc in self.delegated_vests_out:
            vests = Amount(self.delegated_vests_out[acc])
            delegated_hp_out[acc] = str(self.hive.vests_to_hp(vests))

        if self.path is None:
            return
        # with open(self.path + 'sbi_delegation_in_'+self.account["name"]+'.txt', 'w') as the_file:
        #    the_file.write(str(delegated_hp_in) + '\n')
        # with open(self.path + 'sbi_delegation_out_'+self.account["name"]+'.txt', 'w') as the_file:
        #    the_file.write(str(delegated_hp_out) + '\n')

    def parse_transfer_out_op(self, op):
        amount = Amount(op["amount"], blockchain_instance=self.hive)
        index = op["index"]
        account = op["from"]
        timestamp = op["timestamp"]
        encrypted = False
        processed_memo = (
            ascii(op["memo"]).replace("\n", "").replace("\\n", "").replace("\\", "")
        )
        if (
            len(processed_memo) > 2
            and (
                processed_memo[0] == "#"
                or processed_memo[1] == "#"
                or processed_memo[2] == "#"
            )
            and account == "steembasicincome"
        ):
            if processed_memo[1] == "#":
                processed_memo = processed_memo[1:-1]
            elif processed_memo[2] == "#":
                processed_memo = processed_memo[2:-2]
            memo = Memo(account, op["to"], blockchain_instance=self.hive)
            processed_memo = ascii(memo.decrypt(processed_memo)).replace("\n", "")
            encrypted = True

        if amount.amount < 1:
            data = {
                "index": index,
                "sender": account,
                "to": op["to"],
                "memo": processed_memo,
                "encrypted": encrypted,
                "referenced_accounts": None,
                "amount": amount.amount,
                "amount_symbol": amount.symbol,
                "timestamp": timestamp,
            }
            self.transactionOutStorage.add(data)
            return
        if amount.symbol == self.hive.hbd_symbol:
            # self.trxStorage.get_account(op["to"], share_type="SBD")
            shares = -int(amount.amount)
            if "http" in op["memo"] or self.hive.hive_symbol not in op["memo"]:
                data = {
                    "index": index,
                    "sender": account,
                    "to": op["to"],
                    "memo": processed_memo,
                    "encrypted": encrypted,
                    "referenced_accounts": None,
                    "amount": amount.amount,
                    "amount_symbol": amount.symbol,
                    "timestamp": timestamp,
                }
                self.transactionOutStorage.add(data)
                return
            trx = self.trxStorage.get_SBD_transfer(
                op["to"],
                shares,
                formatTimeString(op["timestamp"]),
                SBD_symbol=self.hive.hbd_symbol,
            )
            sponsee = json.dumps({})
            if trx:
                sponsee = trx["sponsee"]
            self.new_transfer_record(
                op["index"],
                processed_memo,
                op["to"],
                op["to"],
                sponsee,
                shares,
                op["timestamp"],
                share_type="Refund",
            )
            # self.new_transfer_record(op["index"], op["to"], "", shares, op["timestamp"], share_type="Refund")
            data = {
                "index": index,
                "sender": account,
                "to": op["to"],
                "memo": processed_memo,
                "encrypted": encrypted,
                "referenced_accounts": sponsee,
                "amount": amount.amount,
                "amount_symbol": amount.symbol,
                "timestamp": timestamp,
            }
            self.transactionOutStorage.add(data)
            return

        else:
            data = {
                "index": index,
                "sender": account,
                "to": op["to"],
                "memo": processed_memo,
                "encrypted": encrypted,
                "referenced_accounts": None,
                "amount": amount.amount,
                "amount_symbol": amount.symbol,
                "timestamp": timestamp,
            }
            self.transactionOutStorage.add(data)
            return

    def parse_transfer_in_op(self, op):
        amount = Amount(op["amount"], blockchain_instance=self.hive)
        share_type = "Standard"
        index = op["index"]
        account = op["from"]
        timestamp = op["timestamp"]
        sponsee = {}
        processed_memo = (
            ascii(op["memo"]).replace("\n", "").replace("\\n", "").replace("\\", "")
        )
        if (
            len(processed_memo) > 2
            and (
                processed_memo[0] == "#"
                or processed_memo[1] == "#"
                or processed_memo[2] == "#"
            )
            and account == "steembasicincome"
        ):
            if processed_memo[1] == "#":
                processed_memo = processed_memo[1:-1]
            elif processed_memo[2] == "#":
                processed_memo = processed_memo[2:-2]
            memo = Memo(account, op["to"], blockchain_instance=self.hive)
            processed_memo = ascii(memo.decrypt(processed_memo)).replace("\n", "")

        shares = int(amount.amount)
        if processed_memo.lower().replace(",", "  ").replace('"', "") == "":
            self.new_transfer_record(
                index,
                processed_memo,
                account,
                account,
                json.dumps(sponsee),
                shares,
                timestamp,
            )
            return
        [sponsor, sponsee, not_parsed_words, account_error] = (
            self.memo_parser.parse_memo(processed_memo, shares, account)
        )
        if amount.amount < 1:
            data = {
                "index": index,
                "sender": account,
                "to": self.account["name"],
                "memo": processed_memo,
                "encrypted": False,
                "referenced_accounts": sponsor + ";" + json.dumps(sponsee),
                "amount": amount.amount,
                "amount_symbol": amount.symbol,
                "timestamp": timestamp,
            }
            self.transactionStorage.add(data)
            return
        if amount.symbol == self.hive.hbd_symbol:
            share_type = self.hive.hbd_symbol

        # Check if any sponsee is the same as the sponsor and remove them
        filtered_sponsee = {}
        for a in sponsee:
            if a != sponsor:  # Only keep sponsees that are not the sponsor
                filtered_sponsee[a] = sponsee[a]
            else:
                print(f"Removed self-sponsorship attempt by {sponsor}")

        # Replace original sponsee dict with filtered one
        sponsee = filtered_sponsee

        sponsee_amount = 0
        for a in sponsee:
            sponsee_amount += sponsee[a]

        if sponsee_amount == 0 and not account_error and True:
            sponsee_account = self.get_highest_avg_share_age_account()
            # Check if sponsee_account is None or same as sponsor
            if sponsee_account is None or sponsee_account == sponsor:
                # If no valid sponsee account is available or it's the same as sponsor,
                # use LessOrNoSponsee status
                sponsee = {}
                _message = (
                    f"{op['timestamp']} to: {self.account['name']} from: {sponsor} "
                    f"amount: {amount} memo: {processed_memo}\n"
                )
                self.new_transfer_record(
                    index,
                    processed_memo,
                    account,
                    sponsor,
                    json.dumps(sponsee),
                    shares,
                    timestamp,
                    status="LessOrNoSponsee",
                    share_type=share_type,
                )
                return
            else:
                # Normal processing with valid sponsee account
                sponsee = {sponsee_account: shares}
                print(
                    "%s sponsers %s with %d shares" % (sponsor, sponsee_account, shares)
                )
                self.new_transfer_record(
                    index,
                    processed_memo,
                    account,
                    sponsor,
                    json.dumps(sponsee),
                    shares,
                    timestamp,
                    share_type=share_type,
                )
                self.memberStorage.update_avg_share_age(sponsee_account, 0)
                self.member_data[sponsee_account]["avg_share_age"] = 0
                return
        elif sponsee_amount == 0 and not account_error:
            sponsee = {}
            _message = (
                f"{op['timestamp']} to: {self.account['name']} from: {sponsor} "
                f"amount: {amount} memo: {processed_memo}\n"
            )
            self.new_transfer_record(
                index,
                processed_memo,
                account,
                sponsor,
                json.dumps(sponsee),
                shares,
                timestamp,
                status="LessOrNoSponsee",
                share_type=share_type,
            )
            return
        if sponsee_amount != shares and not account_error and True:
            sponsee_account = self.get_highest_avg_share_age_account()
            sponsee_shares = shares - sponsee_amount
            if sponsee_shares > 0 and sponsee_account is not None:
                sponsee = {sponsee_account: sponsee_shares}
                print(
                    "%s sponsers %s with %d shares"
                    % (sponsor, sponsee_account, sponsee_shares)
                )
                self.new_transfer_record(
                    index,
                    processed_memo,
                    account,
                    sponsor,
                    json.dumps(sponsee),
                    shares,
                    timestamp,
                    share_type=share_type,
                )
                self.memberStorage.update_avg_share_age(sponsee_account, 0)
                self.member_data[sponsee_account]["avg_share_age"] = 0
                return
            else:
                sponsee = {}
                self.new_transfer_record(
                    index,
                    processed_memo,
                    account,
                    sponsor,
                    json.dumps(sponsee),
                    shares,
                    timestamp,
                    status="LessOrNoSponsee",
                    share_type=share_type,
                )
                return
        elif sponsee_amount != shares and not account_error:
            _message = (
                f"{op['timestamp']} to: {self.account['name']} from: {sponsor} "
                f"amount: {amount} memo: {ascii(op['memo'])}\n"
            )
            self.new_transfer_record(
                index,
                processed_memo,
                account,
                sponsor,
                json.dumps(sponsee),
                shares,
                timestamp,
                status="LessOrNoSponsee",
                share_type=share_type,
            )

            return
        if account_error:
            _message = (
                f"{op['timestamp']} to: {self.account['name']} from: {sponsor} "
                f"amount: {amount} memo: {ascii(op['memo'])}\n"
            )
            self.new_transfer_record(
                index,
                processed_memo,
                account,
                sponsor,
                json.dumps(sponsee),
                shares,
                timestamp,
                status="AccountDoesNotExist",
                share_type=share_type,
            )

            return

        self.new_transfer_record(
            index,
            processed_memo,
            account,
            sponsor,
            json.dumps(sponsee),
            shares,
            timestamp,
            share_type=share_type,
        )

    def _add_audit_log(self, account, value_type, old_value, new_value, reason, related_trx_id=None):
        if self.auditStorage is None:
            return
        if old_value == new_value:
            return
        audit_log = {
            "account": account,
            "value_type": value_type,
            "old_value": old_value,
            "new_value": new_value,
            "change_amount": new_value - old_value,
            "timestamp": datetime.now(),
            "reason": reason,
            "related_trx_id": related_trx_id,
        }
        self.auditStorage.add(audit_log)

    def _handle_point_transfer(self, op):
        # Must be an incoming transfer to steembasicincome of amount between 0.005 and 1
        amount_obj = Amount(op["amount"], blockchain_instance=self.hive)
        amount = float(amount_obj)
        if not (amount >= 0.005 and amount < 1):
            return False

        sender = op["from"]

        # Decrypt/normalize memo similar to parse_transfer_in_op
        processed_memo = (
            ascii(op["memo"]).replace("\n", "").replace("\\n", "").replace("\\", "")
        )
        if (
            len(processed_memo) > 2
            and (
                processed_memo[0] == "#" or processed_memo[1] == "#" or processed_memo[2] == "#"
            )
            and sender == "steembasicincome"
        ):
            # Unlikely for incoming, but keep parity with parse_transfer_in_op
            if processed_memo[1] == "#":
                processed_memo = processed_memo[1:-1]
            elif processed_memo[2] == "#":
                processed_memo = processed_memo[2:-2]
            memo = Memo(sender, op["to"], blockchain_instance=self.hive)
            processed_memo = ascii(memo.decrypt(processed_memo)).replace("\n", "")

        # Normalize and extract nominee (first token, lowercase, strip @)
        memo_norm = " ".join(str(processed_memo).split()).strip().lower()
        nominee = memo_norm.split()[0] if memo_norm else ""
        if nominee.startswith("@"):  # strip leading @
            nominee = nominee[1:]

        if sender not in self.member_data:
            return False
        if nominee not in self.member_data:
            return False
        if sender == nominee:
            return False

        sender_member = self.member_data[sender]
        nominee_member = self.member_data[nominee]

        if amount_obj.symbol == "HBD":
            old_sender_shares = sender_member.get("shares", 0)
            old_nominee_shares = nominee_member.get("shares", 0)
            units = int(amount * 1000)

            if old_sender_shares < units:
                units = old_sender_shares

            if units <= 0:
                return False

            if "shares" not in sender_member:
                sender_member["shares"] = 0
            if "shares" not in nominee_member:
                nominee_member["shares"] = 0

            sender_member["shares"] -= units
            nominee_member["shares"] += units

            self.memberStorage.update(sender_member)
            self.memberStorage.update(nominee_member)

            self._add_audit_log(
                sender,
                "shares",
                old_sender_shares,
                sender_member["shares"],
                f"Transferred {units} HSBI units to {nominee}",
                op.get("trx_id"),
            )
            self._add_audit_log(
                nominee,
                "shares",
                old_nominee_shares,
                nominee_member["shares"],
                f"Received {units} HSBI units from {sender}",
                op.get("trx_id"),
            )

            # Log sender (negative shares) in trx for reports
            base_index = op.get("index", 0) or op.get("op_acc_index", 0) or 0
            sponsee_json = json.dumps({nominee: units})
            timestamp = op.get("timestamp")
            self.trxStorage.add(
                {
                    "index": base_index,
                    "source": self.account["name"],
                    "memo": f"Transfer to {nominee}",
                    "account": sender,
                    "sponsor": sender,
                    "sponsee": sponsee_json,
                    "shares": -units,
                    "vests": 0.0,
                    "timestamp": formatTimeString(timestamp),
                    "status": "Valid",
                    "share_type": "Transfer",
                }
            )
            return True
        else:
            # Convert micro-amount equivalent to rshares
            old_sender_rshares = sender_member["balance_rshares"]
            old_nominee_rshares = nominee_member["balance_rshares"]

            hbd_equiv = amount * 1000
            points = int(hbd_equiv * float(self.rshares_per_hbd or 1))

            if old_sender_rshares < points:
                points = old_sender_rshares

            if points <= 0:
                return False

            sender_member["balance_rshares"] -= points
            nominee_member["balance_rshares"] += points

            self.memberStorage.update(sender_member)
            self.memberStorage.update(nominee_member)

            self._add_audit_log(
                sender,
                "balance_rshares",
                old_sender_rshares,
                sender_member["balance_rshares"],
                f"Transferred {points} rshares to {nominee}",
                op.get("trx_id"),
            )
            self._add_audit_log(
                nominee,
                "balance_rshares",
                old_nominee_rshares,
                nominee_member["balance_rshares"],
                f"Received {points} rshares from {sender}",
                op.get("trx_id"),
            )

            return True

    def new_transfer_record(
        self,
        index,
        memo,
        account,
        sponsor,
        sponsee,
        shares,
        timestamp,
        status="Valid",
        share_type="Standard",
    ):
        data = {
            "index": index,
            "source": self.account["name"],
            "memo": memo,
            "account": account,
            "sponsor": sponsor,
            "sponsee": sponsee,
            "shares": shares,
            "vests": float(0),
            "timestamp": formatTimeString(timestamp),
            "status": status,
            "share_type": share_type,
        }
        self.trxStorage.add(data)

    def new_delegation_record(
        self, index, account, vests, timestamp, status="Valid", share_type="Delegation"
    ):
        data = {
            "index": index,
            "source": self.account["name"],
            "memo": "",
            "account": account,
            "sponsor": account,
            "sponsee": json.dumps({}),
            "shares": 0,
            "vests": float(vests),
            "timestamp": formatTimeString(timestamp),
            "status": status,
            "share_type": share_type,
        }
        self.trxStorage.add(data)

    def parse_op(self, op, parse_vesting=True):
        if op["type"] == "delegate_vesting_shares" and parse_vesting:
            vests = Amount(op["vesting_shares"], blockchain_instance=self.hive)
            # print(op)
            if op["delegator"] == self.account["name"]:
                delegation = {"account": op["delegatee"], "amount": vests}
                self.update_delegation(op, 0, delegation)
                return
            if op["delegatee"] == self.account["name"]:
                delegation = {"account": op["delegator"], "amount": vests}
                self.update_delegation(op, delegation, 0)
                return

        elif op["type"] == "transfer":
            _amount = Amount(op["amount"], blockchain_instance=self.hive)
            # Outgoing transfers from this account (except excluded)
            if (
                op["from"] == self.account["name"]
                and op["to"] not in self.excluded_accounts
            ):
                self.parse_transfer_out_op(op)

            # Incoming transfers to this account (except excluded)
            if (
                op["to"] == self.account["name"]
                and op["from"] not in self.excluded_accounts
            ):
                # If this account is one of the SBI accounts, enforce URL-promotion skip first
                sbi_accounts = {
                    "steembasicincome",
                    "sbi2",
                    "sbi3",
                    "sbi4",
                    "sbi5",
                    "sbi6",
                    "sbi7",
                    "sbi8",
                    "sbi9",
                    "sbi10",
                }
                if self.account["name"] in sbi_accounts:
                    memo_str = str(op.get("memo", ""))
                    if memo_str[:8] == "https://":
                        return
                    # Point transfer window: <1 and >= 0.005
                    amt_float = float(_amount)
                    if amt_float < 1:
                        # Try to handle as point transfer; if not eligible, log as <1 transfer like before
                        if amt_float >= 0.005 and self.memberStorage is not None:
                            processed = self._handle_point_transfer(op)
                            if processed:
                                return
                        # Not processed as point transfer; fall back to logging the <1 transfer
                        self.parse_transfer_in_op(op)
                        return
                    # For >= 1, normal parse
                    self.parse_transfer_in_op(op)
                    return
                else:
                    # Non-SBI accounts: normal behavior
                    self.parse_transfer_in_op(op)
                    return

            return

    def add_mngt_shares(self, last_op, mgnt_shares, op_count):
        timestamp = last_op["timestamp"]
        sponsee = {}
        latest_share = self.trxStorage.get_lastest_share_type("Mgmt")
        if latest_share is not None:
            start_index = latest_share["index"] + 1
        else:
            start_index = op_count / 100 * 3
        for account in mgnt_shares:
            shares = mgnt_shares[account]
            sponsor = account
            data = {
                "index": start_index,
                "source": "mgmt",
                "memo": "",
                "account": account,
                "sponsor": sponsor,
                "sponsee": sponsee,
                "shares": shares,
                "vests": float(0),
                "timestamp": formatTimeString(timestamp),
                "status": "Valid",
                "share_type": "Mgmt",
            }
            start_index += 1
            self.trxStorage.add(data)
