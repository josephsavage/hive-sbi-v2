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

from hivesbi.issue import TokenIssuer, issue_default_tokens
from hivesbi.memo_parser import MemoParser
from hivesbi.settings import get_runtime

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
        self._token_issuers: dict[str, TokenIssuer | None] = {}
        # Default ignored accounts (used as fallback if not provided via config)
        default_excluded = [
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
        # Load from settings if available (key: trx_ignore_accounts)
        try:
            cfg = get_runtime()["cfg"]
            ignore_val = cfg.get("trx_ignore_accounts") if hasattr(cfg, "get") else None
            if ignore_val is None and "trx_ignore_accounts" in cfg:
                ignore_val = cfg["trx_ignore_accounts"]
            if isinstance(ignore_val, str):
                # Support comma/space separated strings
                parsed = [
                    x.strip()
                    for x in ignore_val.replace("\n", ",").split(",")
                    if x.strip()
                ]
                self.excluded_accounts = parsed or default_excluded
            elif isinstance(ignore_val, (list, tuple, set)):
                self.excluded_accounts = list(ignore_val) or default_excluded
            else:
                self.excluded_accounts = default_excluded
        except Exception:
            # On any issue, use the defaults to remain robust
            self.excluded_accounts = default_excluded

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
            return
            
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

    def _add_audit_log(
        self, account, value_type, old_value, new_value, reason, related_trx_id=None
    ):
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
        """Process a point transfer and return True if custom handling occurred."""

        amount_obj = Amount(op["amount"], blockchain_instance=self.hive)
        amount = float(amount_obj)
        sender = op["from"]
        trx_id = op.get("trx_id")
        print(
            f"[PointTransfer] Start: trx_id={trx_id} from={sender} to={op.get('to')} amount={amount_obj}"
        )
        if amount < 0.005:
            print(
                f"[PointTransfer] Skip small amount (<0.005): trx_id={trx_id} amount={amount}"
            )
            return False

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
            and sender == "steembasicincome"
        ):
            print(
                f"[PointTransfer] Detected encrypted memo prefix: trx_id={trx_id} memo={processed_memo}"
            )
            if processed_memo[1] == "#":
                processed_memo = processed_memo[1:-1]
            elif processed_memo[2] == "#":
                processed_memo = processed_memo[2:-2]
            memo = Memo(sender, op["to"], blockchain_instance=self.hive)
            processed_memo = (
                ascii(memo.decrypt(processed_memo))
                .replace("\n", "")
                .replace("\\n", "")
                .replace("\\", "")
            )
            print(
                f"[PointTransfer] Decrypted memo: trx_id={trx_id} memo={processed_memo}"
            )
        memo_norm = " ".join(str(processed_memo).split()).strip().lower()
        nominee = memo_norm.split()[0] if memo_norm else ""
        # Strip leading @ and any trailing punctuation commonly found in memos, then lowercase
        if nominee.startswith("@"):
            nominee = nominee[1:]
        nominee = nominee.strip(" ,.;:!?'\"()[]{}").lower()
        print(
            f"[PointTransfer] Memo parsed: trx_id={trx_id} nominee={nominee} memo_norm={memo_norm}"
        )

        if sender not in self.member_data:
            print(
                f"[PointTransfer] Sender not in member_data: trx_id={trx_id} sender={sender}"
            )
            return False
        if nominee not in self.member_data:
            print(
                f"[PointTransfer] Nominee not in member_data: trx_id={trx_id} nominee={nominee}"
            )
            return False
        if sender == nominee:
            print(
                f"[PointTransfer] Sender and nominee identical: trx_id={trx_id} account={sender}"
            )
            return False

        sender_member = self.member_data[sender]
        nominee_member = self.member_data[nominee]

        # Uncapped HBD unit transfer
        if amount_obj.symbol == "HBD":
            old_sender_shares = sender_member.get("shares", 0)
            old_nominee_shares = nominee_member.get("shares", 0)
            total_units = int(round(amount * 1000))
            if total_units <= 0:
                print(
                    f"[PointTransfer] Non-positive total_units: trx_id={trx_id} total_units={total_units}"
                )
                return False

            transferable_units = min(total_units, max(old_sender_shares, 0))
            refunded_units = total_units - transferable_units
            print(
                "[PointTransfer] HBD transfer calc: "
                f"trx_id={trx_id} total_units={total_units} transferable={transferable_units} "
                f"refunded={refunded_units} sender_shares={old_sender_shares} nominee_shares={old_nominee_shares}"
            )

            # Refund any excess units if transferable units are zero
            if transferable_units <= 0:
                print(
                    f"[PointTransfer] Refund all units (no transferable): trx_id={trx_id} refund_units={total_units}"
                )
                self._refund_excess_transfer(
                    recipient=sender,
                    refund_units=total_units,
                    symbol=amount_obj.symbol,
                    nominee=nominee,
                    op=op,
                )
                return False

            sender_member.setdefault("shares", 0)
            nominee_member.setdefault("shares", 0)

            sender_member["shares"] -= transferable_units
            nominee_member["shares"] += transferable_units
            print(
                f"[PointTransfer] Updated shares: trx_id={trx_id} sender_shares={sender_member['shares']} "
                f"nominee_shares={nominee_member['shares']}"
            )

            self.memberStorage.update(sender_member)
            self.memberStorage.update(nominee_member)

            self._add_audit_log(
                sender,
                "shares",
                old_sender_shares,
                sender_member["shares"],
                f"Transferred {transferable_units} HSBI units to {nominee}",
                op.get("trx_id"),
            )
            self._add_audit_log(
                nominee,
                "shares",
                old_nominee_shares,
                nominee_member["shares"],
                f"Received {transferable_units} HSBI units from {sender}",
                op.get("trx_id"),
            )

            base_index = op.get("index", 0) or op.get("op_acc_index", 0) or 0
            sponsee_json = json.dumps({nominee: transferable_units})
            timestamp = op.get("timestamp")
            self.trxStorage.add(
                {
                    "index": base_index,
                    "source": self.account["name"],
                    "memo": f"Transfer to {nominee}",
                    "account": sender,
                    "sponsor": sender,
                    "sponsee": sponsee_json,
                    "shares": -transferable_units,
                    "vests": 0.0,
                    "timestamp": formatTimeString(timestamp),
                    "status": "Valid",
                    "share_type": "Transfer",
                }
            )

            # Issue default tokens to the sender if nominee is sbi-tokens
            if nominee == "sbi-tokens":
                token_recipient = sender
                print(
                    f"[PointTransfer] Issuing default tokens: trx_id={trx_id} recipient={token_recipient} "
                    f"units={transferable_units}"
                )
                try:
                    issue_default_tokens(token_recipient, transferable_units)
                except Exception:
                    log.exception(
                        "Failed to issue default tokens for %s (%s units)",
                        token_recipient,
                        transferable_units,
                    )
                else:
                    log.info("Issued %d HSBI tokens to %s", transferable_units, sender)

            # Refund any excess units if any
            if refunded_units > 0:
                print(
                    f"[PointTransfer] Refunding excess units: trx_id={trx_id} refund_units={refunded_units}"
                )
                self._refund_excess_transfer(
                    recipient=sender,
                    refund_units=refunded_units,
                    symbol=amount_obj.symbol,
                    nominee=nominee,
                    op=op,
                )

            print(
                f"[PointTransfer] Completed HBD transfer: trx_id={trx_id} nominee={nominee} units={transferable_units}"
            )
            return True

        # HIVE rshares transfer a.k.a Lovegun point transfer
        old_sender_rshares = sender_member["balance_rshares"]
        old_nominee_rshares = nominee_member["balance_rshares"]
        hbd_equiv = amount * 1000
        points = int(hbd_equiv * float(self.rshares_per_hbd or 1))

        if old_sender_rshares < points:
            points = old_sender_rshares

        if points <= 0:
            print(
                f"[PointTransfer] Non-positive rshare points: trx_id={trx_id} points={points} sender_rshares={old_sender_rshares}"
            )
            return False

        sender_member["balance_rshares"] -= points
        nominee_member["balance_rshares"] += points
        print(
            f"[PointTransfer] Rshares updated: trx_id={trx_id} sender_rshares={sender_member['balance_rshares']} "
            f"nominee_rshares={nominee_member['balance_rshares']} points={points}"
        )

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

        print(
            f"[PointTransfer] Completed rshares transfer: trx_id={trx_id} nominee={nominee} points={points}"
        )
        return True

    def _get_token_issuer(self, account_name: str) -> TokenIssuer | None:
        issuer = self._token_issuers.get(account_name)
        if issuer is not None or account_name in self._token_issuers:
            return issuer
        try:
            issuer = TokenIssuer(account_name=account_name)
        except Exception as exc:
            log.warning(
                "Unable to initialize TokenIssuer for %s: %s",
                account_name,
                exc,
            )
            issuer = None
        self._token_issuers[account_name] = issuer
        return issuer

    def _refund_excess_transfer(
        self,
        recipient: str,
        refund_units: int,
        symbol: str,
        nominee: str,
        op: dict,
    ) -> None:
        if refund_units <= 0:
            return

        amount_value = round(refund_units / 1000, 3)
        if amount_value <= 0:
            return

        issuer = self._get_token_issuer(self.account["name"])
        if issuer is None:
            log.warning(
                "Skipping refund of %.3f %s to %s; no issuer for %s",
                amount_value,
                symbol,
                recipient,
                self.account["name"],
            )
            return

        memo = (
            f"Refund: excess {symbol} for share transfer to {nominee}"
            if nominee
            else "Refund: excess transfer"
        )
        try:
            issuer.transfer(recipient, amount_value, asset_symbol=symbol, memo=memo)
        except Exception as exc:
            log.warning(
                "Failed to refund %.3f %s to %s for trx %s: %s",
                amount_value,
                symbol,
                recipient,
                op.get("trx_id"),
                exc,
            )

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
                    amt_float = float(_amount)

                    # HBD unit transfer
                    if _amount.symbol == "HBD" and self.account["name"] == "steembasicincome":
                        if amt_float >= 0.005 and self.memberStorage is not None:
                            processed = self._handle_point_transfer(op)
                            if processed:
                                return
                        # Not processed as units transfer; fall back to normal logging
                        self.parse_transfer_in_op(op)
                        return

                    # Lovegun point-transfer flow for HIVE
                    if amt_float < 1 and self.account["name"] == "steembasicincome":
                        if amt_float >= 0.005 and self.memberStorage is not None:
                            processed = self._handle_point_transfer(op)
                            if processed:
                                return
                        self.parse_transfer_in_op(op)
                        return

                    # Normal transfer
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
