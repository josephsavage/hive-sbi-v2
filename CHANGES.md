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
- **A stuck row is failed from its own recorded error, for free.**
  `record_pending_error` stores the raised broadcast error on the row, so a row
  stranded before `record_broadcast_error` existed already carries the proof of its
  own rejection. `fail_pending_with_proven_non_inclusion` applies the same marker
  list to `error_message` in one UPDATE with no network at all, and runs first in
  every reconciliation pass. It is the same decision `record_broadcast_error` makes
  at broadcast time, made late — so adding a marker also retroactively clears
  historical rows carrying that error.
- **The scan window no longer grows.** It used to be widened to cover the oldest
  PENDING row, which scaled the op budget with it and pinned it at its ceiling on
  every cycle for as long as one unresolvable row existed — tens of thousands of
  ops (`history_reverse` batches 1000 per call) re-walked each pass to rediscover
  a single row's fate. The lookback is now fixed at `CHAIN_SCAN_LOOKBACK`, so an
  ordinary cycle exits at `reached_cutoff` after a few hundred ops.
  `HISTORY_SCAN_HARD_LIMIT` and the extension are gone.
- **Rows older than the scan's reach are resolved individually against Hive
  Engine.** `resolve_pending_beyond_scan` asks the Hive Engine history API whether
  one specific issuance exists, scoped to the recipient, the symbol and that row's
  own 35-minute match window (`fetch_issues_to_recipient`). Cost is one small
  request per stuck row regardless of its age, where the bulk scan's reach is
  bounded by issuer op volume. Reaching this path means the row carries no proof of
  its own fate — the sweep above has already failed everything that does — so what
  is left is a timeout, a transport failure, or a process that died between the
  write-ahead insert and either outcome write.
  - The row's window is re-checked **locally** rather than trusted to the remote
    `timestampStart`/`timestampEnd`: the history endpoint is chosen from a beacon at
    runtime, and a node that silently ignored those params would return the
    recipient's most recent issues instead. Matching one of those would mark the row
    SUCCESS and debit a balance against an issuance never made for it.
  - Lookups run **outside** the reconciliation transaction (each can block for the
    httpx timeout) and are capped at `MAX_BEYOND_SCAN_LOOKUPS` rows **and**
    `BEYOND_SCAN_TIME_BUDGET` of wall clock per pass, least-recently-attempted
    first, so a backlog drains over consecutive cycles.
  - `covered_since=None` — the scan's own history fetch failed — now means "no row
    is covered" rather than "resolve nothing": this path does not depend on that
    scan, and skipping it would let one failing API disable resolution for everyone.
  - Uncertainty leaves the row PENDING: untrusted response, unelapsed match
    window, an issue outside the row's window, or an uncaptured sibling with a claim
    on it. Multiple candidates are resolved by proximity, not deferred — see below.
- **Broadcast pacing moved to the issuer.** The 5-per-block budget belongs to the
  issuer account across the whole process, not to any one loop, so the previous
  "sleep 3s every 5 issuances" inside `issue_balance_tokens` could not enforce it —
  the pik loop, the Pending Balance Conversion loop, Management and Unit Conversion
  all broadcast from `hivesbi`. `hivesbi.issue.throttle_broadcast` now spaces every
  Hive Engine broadcast by `BROADCAST_MIN_INTERVAL` (1s, so at most 3 ops per
  block), measured from the start of the previous broadcast so a slow round trip
  pays no extra tax. `get_default_token_issuer` is now genuinely cached, as its
  docstring always claimed.

## Token issuance: code-review fixes on the above (PR #140)

Four of these were paths to **double-minting HSBIDAO**: a PENDING row wrongly
marked FAILURE is re-issued next cycle while the member's balance was never
debited, and an on-chain mint cannot be undone.

- **The PENDING sweep no longer matches by SQL `LIKE`.**
  `fail_pending_with_proven_non_inclusion` interpolated each marker into
  `error_message LIKE '%…%'`, where `_` is a single-character wildcard — so
  `HIVE_CUSTOM_OP_BLOCK_LIMIT` also matched messages `never_reached_chain()`
  rejects, and the two paths were not the "single bar" the docstring claimed. The
  sweep now selects PENDING rows and applies `never_reached_chain` in Python, so
  the predicate is literally shared, then updates by primary key. That also drops
  an unindexed full-table UPDATE that took next-key locks across
  `token_issuance_log` while the unified webserver read it.
- **An exhausted history walk no longer claims coverage of all time.** When the
  generator ended before reaching the cutoff, the scan reported
  `covered_since = datetime.min` on the theory that a history which ended was
  walked in full. But an exhausted iterator and one that stopped early are
  indistinguishable from there, and `HISTORY_SCAN_LIMIT` is exactly 10× nectar's
  1000-op `history_reverse` batch, so a batch-boundary stop landed in that branch
  by construction — failing every settled PENDING row in a single pass. Coverage
  now always ends at the oldest op actually inspected; older rows fall through to
  the per-row lookup, which is what it exists for.
- **The Hive Engine history lookup pages past the endpoint's silent clamp.**
  `accountHistory` clamps `limit` to 500 — asking for 1000 returns exactly 500 rows
  with no error and no truncation indicator (verified against both beacon
  endpoints), so a cut-off response read as "nothing was issued". It now requests
  the clamp and pages on `offset` until a short page proves the result set is
  exhausted; a result needing more than `MAX_HISTORY_PAGES` reads as **unknown**
  and leaves the row PENDING.
- **Two rows sharing a window no longer deadlock as PENDING.** With more than one
  candidate, `resolve_pending_beyond_scan` deferred. Deferring is correct in the
  bulk matcher — a contested row resolves once its sibling records a trx_id — but
  nothing else ever moves a beyond-scan row, so two rows for the same recipient and
  amount with overlapping windows each saw both issues, each deferred, and stayed
  PENDING forever: the "member silently stops being paid" symptom this path exists
  to cure. It now takes the candidate nearest its own intent timestamp (the bulk
  matcher's discriminator, shared via `_rank_by_proximity`), breaking exact ties by
  trx_id. Every candidate carries the same units by construction, so a swapped
  attribution costs trx_id accuracy in the audit trail, never tokens.

And on availability and correctness of attribution:

- **The uncaptured-sibling guard covers the sibling's own reach.** A sibling claims
  any issue in `[its issued_at - MATCH_CLOCK_SKEW, its issued_at + CHAIN_MATCH_WINDOW]`,
  so its claim overlaps ours whenever its `issued_at` is within
  `CHAIN_MATCH_WINDOW + MATCH_CLOCK_SKEW`. Scoping to `CHAIN_MATCH_WINDOW` alone
  left a 5-minute blind spot at each end in which a shared issue was handed to the
  PENDING row, stamping it with the sibling's trx_id and debiting a balance for
  tokens never issued against it.
- **One pass is bounded in time, not just in rows.** `MAX_BEYOND_SCAN_LOOKUPS`
  bounds requests; a history node that hangs rather than errors turned that into
  cap × timeout (200 × 30s ≈ 100 minutes). `sbirunner.sh` is sequential, so that
  came straight out of the voting window of every job downstream. There is now a
  `BEYOND_SCAN_TIME_BUDGET` (5 min) checked between rows, and the per-request
  timeout dropped from 30s to `HISTORY_REQUEST_TIMEOUT` (10s).
- **Unresolvable rows rotate instead of starving the queue.** Selection was
  oldest-first and capped, so 200 rows that could never be settled consumed every
  lookup every cycle and row 201 never got one — the head-of-line blocking this
  work removed elsewhere, at a threshold of 200 instead of 1. New column
  `token_issuance_log.last_resolution_attempt` is stamped on **every** attempt,
  including those that leave the row PENDING, and selection orders by it
  (never-attempted first).
- **A transient non-200 no longer costs a whole cycle.** The lookup re-implemented
  `nectarengine.Api.get_history`'s plumbing but dropped its retry loop, so one 429
  or 503 meant no progress for that row until the next cycle. It now retries
  `HISTORY_RETRY_ATTEMPTS` times with backoff.
- **`history_url` is resolved once per process.** `Api()` builds an RPC pool and
  consults the beacon (~1.4s cold in the pinned container) and was constructed once
  per stuck row for a value that never changes within a run.
- **The token symbol travels with the issuer.** `reconcile_recent_issuances`
  forwarded `issuer.account_name` but let the symbol default, so a non-HSBIDAO
  issuer would have had its rows checked against HSBIDAO history, found nothing,
  and failed every one of them.
- Housekeeping: `warn_stuck_pending`'s docstring no longer blames an incomplete
  chain scan (truncation stopped being a cause when coverage became per-row), and
  the `BATCH_SLEEP_TIME` sleeps in `main()` are gone — they were the loop-granular
  half of the pacing scheme `throttle_broadcast` replaced, adding 6s a cycle for
  nothing. `BROADCAST_MIN_INTERVAL` stays at 1.0s, but now documents its latency
  cost (~12.8 min/cycle at peak vs ~7.7 min under the old 0.6 s/issuance) rather
  than only its block-budget headroom.

**Schema change** — `token_issuance_log.last_resolution_attempt` plus
`idx_status_attempt (status, last_resolution_attempt, issued_at)`. The index also
gives the PENDING sweep and `warn_stuck_pending` something better than a full scan
of a 159k-row table that grows every cycle. Prod DDL is **step 5 of
`sql/PROD_RUNBOOK_virtual_tokens.sql`**, with its own pre-/post-checks; run it
after step 4 so the new column lands at the end of the row and applies instantly.
**It must be applied before deploying this code** — `resolve_pending_beyond_scan`
orders by the new column, so every `status = 'PENDING'` query errors on the unknown
column without it. Dev picks both up from `docker/mariadb/init/01-sbi-schema.sql`
on a fresh volume (`docker compose down -v`).

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
