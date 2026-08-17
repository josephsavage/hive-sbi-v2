# Changes from bb3ac15046e999e1ad076d84241186135e084757 to HEAD

## Token issuance: block-limit stall

Symptom: an account with regular Pending Balance Conversions stopped being paid.
Its last attempt held `error_message = "Assert Exception:insert_info.first->second
<= HIVE_CUSTOM_OP_BLOCK_LIMIT: Account hivesbi already submitted 5 custom json
operation(s) this block."`, stayed PENDING for a week, and blocked every later
conversion for that member (`issue_balance_tokens` skips a member holding a live
PENDING row for the same rationale).

- **A provably-rejected broadcast now fails its row immediately.** Hive asserts
  away the 6th `custom_json` an account submits in one block, so that operation can
  never appear on chain and there is nothing for reconciliation to discover.
  `record_broadcast_error` fails the row on such errors and the next cycle simply
  re-issues; balances are untouched, so the retry pays the full amount. Ambiguous
  errors (timeouts, transport failures, anything unrecognized) still stay PENDING —
  failing a row that did reach the chain would mint the tokens twice. The marker
  list is `hivesbi.issuance_log.NEVER_REACHED_CHAIN_MARKERS`; the bar for adding to
  it is that the node rejected the operation deterministically before inclusion.
- **One unreachable PENDING row no longer blocks every other row.** A scan that ran
  out of op budget before reaching its cutoff used to report
  `covered_since=None`/`complete=False`, and `reconcile_issuances` gated *all*
  failure-resolution on that scan-wide flag. Because the lookback is extended to
  cover the oldest PENDING row, one row older than the scan could reach made every
  later pass truncate — so nothing could ever resolve, for anyone, and the condition
  reinforced itself as the row aged. A truncated scan now reports the span it
  actually walked (`covered_since = oldest op inspected + one block`), coverage is
  judged per row, and the `scan_complete` parameter is gone.
- **The lookback extension is capped** at `MAX_CHAIN_SCAN_LOOKBACK` (7 days) so it
  cannot grow until the scan stops finishing.
- **Rows older than the scan's reach are resolved individually against Hive
  Engine.** `resolve_pending_beyond_scan` asks the Hive Engine history API whether
  one specific issuance exists, scoped to the recipient, the symbol and that row's
  own 35-minute match window (`fetch_issues_to_recipient`). Cost is one small
  request per stuck row regardless of its age, where the bulk scan's reach is
  bounded by issuer op volume. Any uncertainty — untrusted response, unelapsed match
  window, more than one candidate — leaves the row PENDING.
- **Broadcast pacing moved to the issuer.** The 5-per-block budget belongs to the
  issuer account across the whole process, not to any one loop, so the previous
  "sleep 3s every 5 issuances" inside `issue_balance_tokens` could not enforce it —
  the pik loop, the Pending Balance Conversion loop, Management and Unit Conversion
  all broadcast from `hivesbi`. `hivesbi.issue.throttle_broadcast` now spaces every
  Hive Engine broadcast by `BROADCAST_MIN_INTERVAL` (1s, so at most 3 ops per
  block), measured from the start of the previous broadcast so a slow round trip
  pays no extra tax. `get_default_token_issuer` is now genuinely cached, as its
  docstring always claimed.

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

## Token issuance: reconciliation hardening (PR #139 review fixes)

- **Contested rows are never failed.** A PENDING row with any in-window chain
  candidate (tie-skipped, or attributed to a sibling row by timestamp proximity) is
  left PENDING even when the scan covers its window — that issue may be its own
  broadcast, so failing it could re-issue tokens already minted. Liveness is
  preserved: once the sibling SUCCESS row records the chain `trx_id`, the next pass
  excludes that issue and the contested row resolves normally.
- **Scan coverage extends to scan time.** `history_reverse` starts at the account
  head, so a successful fetch proves no ops newer than the newest scanned op exist;
  `covered_until` is now the scan start minus the clock-skew margin instead of the
  newest op timestamp. A quiet issuer (no recent on-chain activity) can therefore
  still resolve stale PENDING rows, and an empty history counts as complete
  coverage. The scan op budget scales with the lookback (capped at
  `HISTORY_SCAN_HARD_LIMIT`) so an old stuck PENDING row cannot outgrow the scan.
- **Unit Conversion is idempotent per source transaction.** Before inserting the
  intent, the sbi-tokens path checks `source_trx_id` + rationale for an existing
  PENDING/SUCCESS row (`has_issuance_for_source`; FAILURE still allows retry), so a
  reprocessed transfer op can no longer double-mint.
- **Write-ahead helpers moved to `hivesbi/issuance_log.py`**, shared by
  `hivesbi/parse_hist_op.py` and `hsbi_token_snapshot.py` (which re-exports them);
  the dead `now` parameter was removed from `reconcile_issuances`.
- **Management virtual refresh preserves the management account's own delegation
  grant**: `refresh_management_virtual_tokens` adds the derived 10% on top of any
  delegation-derived virtual tokens josephsavage earned as an ordinary delegator,
  instead of overwriting them.
- Runbook: added a DB time-zone pre-check (`issued_at` is a TIMESTAMP matched
  against UTC chain timestamps within ±minutes, so the session time zone must be
  UTC) and a review/cleanup section for legacy `rationale='reconciled'` rows left
  by PR #138.
- Tests: fixed the stale-coverage test (coverage must span
  `issued_at - MATCH_CLOCK_SKEW`) and the equal-distance ambiguity test
  (second-precision `issued_at` requires whole-second timestamps); added coverage
  for contested-row protection + next-pass resolution, quiet-issuer/empty-history
  scan coverage, the source-transaction dedup guard, and Management own-delegation
  preservation.

## Token issuance: write-ahead intents + chain reconciliation (PR #138 follow-up)

- All HSBIDAO issuance paths now use a committed write-ahead intent row before
  broadcast: pik, Pending Balance Conversion, Management, and Unit Conversion insert
  `token_issuance_log.trx_id = 'PENDING'` with durable recipient, units, rationale,
  optional `source_trx_id`, and an app-written UTC `issued_at` intent timestamp. On a
  successful broadcast the same row moves to `SUCCESS` with the actual Hive Engine
  `tokens.issue` transaction id. On broadcast exception the row stays `PENDING` with
  `error_message`, and member balances are preserved for reconciliation or retry.
- `token_issuance_log.trx_id` now means the actual Hive Engine `tokens.issue`
  transaction id. `token_issuance_log.source_trx_id` means the origin transaction id,
  such as the HBD transfer that triggered Unit Conversion. Unit Conversion stores the
  source transfer hash in `source_trx_id` and the returned Hive Engine issue hash in
  `trx_id`; it no longer uses the source transfer hash as the issuance id.
- Reconciliation (`reconcile_issuances`) only completes, fails, or reports existing
  rows. It never inserts a new issuance row and never invents `rationale='reconciled'`.
  Exact `trx_id` matches are already handled; unresolved `PENDING` rows match chain
  issues by recipient, normalized units, and authoritative blockchain transaction
  timestamp relative to the stored intent timestamp. Ambiguous equal-distance matches
  remain unresolved.
- Reconciliation split timing into chain matching and scan coverage. A PENDING row is
  marked `FAILURE` only when the chain scan proves it covered the entire possible match
  range; orphan chain issuances are printed for operator review only.
- `hsbi_check_delegation.py` now refreshes the derived Management virtual token
  allocation every pass: `josephsavage.virtual_tokens = ROUND(SUM(non-josephsavage
  virtual_tokens) * 0.10, 3)`. The Management row is upserted and excluded from the
  source sum to avoid compounding.
- Tests: `tests/test_virtual_tokens.py` covers delegation virtual_tokens + atomicity,
  Management virtual token refresh/upsert, write-ahead issuance success/failure and
  duplicate-pending guards, Unit Conversion source-vs-issue transaction ids,
  timestamp-based reconciliation, stale scan coverage, orphan reporting without insert,
  and the `6fda...` source / `af4...` issue regression.

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
