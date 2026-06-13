# Changes from bb3ac15046e999e1ad076d84241186135e084757 to HEAD

## Delegation rewards → HSBIDAO virtual_tokens (PR #138)

- Delegations no longer grant voting-weight bonus *shares*. On every delegation
  change or new delegation, `hsbi_check_delegation.py` now zeroes the delegation
  `trx` accrual — **both** `shares` and `vests` (`clear_delegation_trx`), since
  `hsbi_update_member_db` recomputes the bonus from `vests` when `shares` is 0 — and
  upserts `tokenholders.virtual_tokens = 2 × delegated_HP` (`calculate_virtual_tokens`,
  Decimal, floored to 0.001). Both writes for a delegator happen in one batched
  transaction so accrual and virtual_tokens never diverge. Removed/leased delegators
  have their `virtual_tokens` zeroed in the same batch.
- New `tokenholders.virtual_tokens decimal(15,3)` column; the generated `tokens`
  column is now `liquid_tokens + LP_tokens + virtual_tokens`, so virtual tokens earn
  PIK dividends like real holdings while staying out of "real circulating supply"
  (`tokens − virtual_tokens`). Schema in `sql/20260602_virtual_tokens.sql` (dev) and
  `sql/PROD_RUNBOOK_virtual_tokens.sql` (hand-applied prod runbook).
- `virtual_tokens` is set at the delegation event and intentionally not refreshed as
  the vests→HP ratio drifts (snapshot-at-delegation; benefits "forgotten" delegations).
- `TrxDB.update_delegation_shares` and `hsbi_check_delegation.calculate_shares` are
  **deprecated** (no longer on any production path) but retained, with docstrings, for
  ad-hoc reporting / historical recomputation.

## Token issuance: Management 10% + chain reconciliation (PR #138)

- `hsbi_token_snapshot.py` now issues the **Management 10%** to `josephsavage` each
  ~2.4 h snapshot cycle, replacing the prior irregular manual issuance. The cap targets
  10% of *real* circulating supply (`SUM(tokens − virtual_tokens)`, excluding
  `sbi-tokens`); `calculate_management_issue_amount` = `(0.10·outstanding − issued) / 0.90`,
  where `issued` counts SUCCESS **and** PENDING. It is **write-ahead**: a committed
  PENDING row counts toward the cap before the broadcast, so a crash after broadcast
  cannot double-mint.
- Chain-confirm reconciliation (`reconcile_issuances`) resolves Management PENDING rows
  against the issuer's recent on-chain HSBIDAO issuances: matched → SUCCESS; genuinely
  absent past the window (only when the scan provably covered the row) → FAILURE so the
  cap frees up. Reconciliation is **rationale-scoped to Management** and recognises
  confirmed pik/abc dividends by logged `trx_id` (and, defensively, recipient+units), so
  a same-amount member dividend to `josephsavage` is never mistaken for a Management
  issuance.
- Per-member **pik / abc_pik issuance stays immediate and self-healing** (no write-ahead
  PENDING guard): a confirmed broadcast zeroes the balance and logs SUCCESS; a failed
  broadcast logs FAILURE and leaves the balance for retry next cycle. This deliberately
  avoids a regression where a transient failure could strand a member's dividends behind
  an unresolved PENDING row (see review finding A). `token_issuance_log.status` gained a
  `PENDING` enum value for the Management path only.
- Tests: `tests/test_virtual_tokens.py` covers delegation virtual_tokens + atomicity, the
  immediate pik/abc success/failure paths, Management cap math + convergence + write-ahead,
  `sync_tokenholders`, and rationale-scoped reconciliation (incl. that a confirmed pik
  chain op is never attributed to Management).

## Memo parsing (PR #138)

- A frontend-host URL whose path is **not** an `/@account` profile (e.g. a post link)
  now surfaces as `account_error` (transfer recorded `AccountDoesNotExist`, visible in
  member-facing reporting) instead of being silently dropped; a bare `http(s)://…`
  single-word memo no longer falls through to the whole-URL account guess. Cases added to
  `tests/test_memo.py` (now patched against a `FakeAccount` so the suite runs offline).

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

- **Management reconciliation depends on a bounded chain scan.** `fetch_recent_chain_issuance_scan`
  reads at most `HISTORY_SCAN_LIMIT` (1000) issuer ops over a 5 h window. At the current
  ~250 issuance events/cycle (~2 cycles, ~500 ops per window) this comfortably reaches the
  cutoff, so a failed Management issuance is marked FAILURE and re-attempted. If per-cycle
  issuance volume grows past that headroom the scan may report `complete=False`, in which
  case a failed Management PENDING simply stays PENDING (cap stays conservative — it never
  double-mints) until a later, smaller window covers it. Revisit the limit/window if
  issuance volume rises materially.
