# Changes from bb3ac15046e999e1ad076d84241186135e084757 to HEAD

## Token issuance and ledger updates

- The default Hive Engine issuer switched to the `hivesbi` account and now points at the `HSBIDAO` token symbol (`hivesbi/issue.py:11-33`). A reusable `get_tokenholders` helper was added to page through Hive Engine balances with multiple `Api.find` call signatures so downstream scripts can snapshot token supply (`hivesbi/issue.py:132-181`).
- `_handle_point_transfer` now pulls the runtime DB handle and, whenever HSBI units are issued/refunded, logs each attempt to a `token_issuance_log` table so on-chain transfers can be reconciled in SQL (`hivesbi/parse_hist_op.py:541-772`). This is also where unit conversion refunds are now performed.
- A new `hsbi_token_snapshot.py` job fetches the live tokenholder list from Hive Engine and upserts it into the local `tokenholders` table while also processing any pending “PIK” issuances stored in that table (`hsbi_token_snapshot.py:19-91`). Successful and failed issuances are logged via the same `token_issuance_log` sink to keep off-chain and on-chain state aligned.

## Mana tracking and accrual control

- `ConfigurationDB` gained `update_max_mana`, which aggregates the `max_mana` column from the `accounts` table into configuration so global mana caps can be tracked (`hivesbi/storage.py:302-309`).
- `hsbi_manage_accrual.py` now records live manabar stats for every managed account each cycle, storing `current_mana`, `max_mana`, `mana_pct`, and a `last_checked` timestamp in the `accounts` table (`hsbi_manage_accrual.py:24-70`). The script compares the fleet-wide mana percentage to the configurable `mana_pct_target` and scales `rshares_per_cycle`/`del_rshares_per_cycle` up or down accordingly before persisting the new thresholds and optionally running reporting stored procedures (`hsbi_manage_accrual.py:31-131`).

## Voting queue, eligibility, and throttling

- Unvoted posts pulled from the DB are now restricted to main posts that are less than 24 hours old, reducing the backlog the voting runner needs to scan (`hivesbi/transfer_ops_storage.py:372-382`).
- `hsbi_upvote_post_comment.py` enforces a minimum voter mana percentage (`mana_pct_target`) and a minimum rshare capacity before accounts are allowed to cast votes, prints an eligibility roster for operators, and removes a voter from the pool once it has consumed roughly 100% of its available voting power (`hsbi_upvote_post_comment.py:34-110` and `hsbi_upvote_post_comment.py:327-360`).
- The post queue is now ordered by the member’s `balance_rshares` (with creation time tie-breaking) so lower-balance members can surface sooner, while rshare sizing, pool assembly, and fallback logic all honor the mana filters (`hsbi_upvote_post_comment.py:117-360`).

## Operational workflow additions

- New automation `hsbi_claim_rewards.py` iterates the configured operator accounts, claims any pending HIVE/HBD/VESTS rewards, mirrors the claimed amounts back into the `accounts` table, and triggers `usp_curation_dividends()` when at least one claim succeeds (`hsbi_claim_rewards.py:12-111`).
- `sbirunner.sh` now schedules the token snapshot and reward-claim jobs ahead of the accrual/voting pipeline so the downstream scripts consume fresh token ledgers and dividend inputs every cycle (`sbirunner.sh:4-12`).

## Fixes applied in this stack

- `hsbi_claim_rewards.py` now imports `KeysDB`, consumes the `AccountsDB` voting list, and falls back to runtime accounts so it always claims for the configured operators (`hsbi_claim_rewards.py:24-40`).
- `_handle_point_transfer` wraps the token-issuance failure log in `except Exception as e`, preventing the previous `NameError` when logging failures (`hivesbi/parse_hist_op.py:717-742`).
- The voting queue sort order now favors higher `balance_rshares` instead of the oldest posts, keeping the workflow testable and aligned with the stated intent (`hsbi_upvote_post_comment.py:117-145`).
- `hsbi_upvote_post_comment.py` removed the unreachable comment-specific branch and now runs only the main-post voting path that matches the `PostsTrx.get_unvoted_post` filter, eliminating dead comment cruft (`hsbi_upvote_post_comment.py:219-359`).

## Issues and regressions to be aware of

- None at this time.
