---
name: token-issuance-design-invariants
description: HSBIDAO issuance rationale values, the one-issuance-per-second guarantee, and the PR #138/#139 history behind them
type: project
---

The lead developer (josephsavage) stated these invariants for HSBIDAO issuance during the PR #139 review on 2026-07-07:

- `token_issuance_log.rationale` records only the reason for an issuance. The valid reasons are PIK, Pending Balance Conversion, Unit Conversion and Management. The constants in `hsbi_token_snapshot.py` (for example `PIK_RATIONALE`, `ABC_RATIONALE`, `MANAGEMENT_RATIONALE`) hold the exact strings. `reconciled` is not a valid rationale.
- One recipient never receives two issuances in the same second. On 2026-07-07 a `BATCH_SLEEP_TIME` stagger gave this guarantee. Since PR #140 (merged 2026-08-22), `throttle_broadcast` in `hivesbi/issue.py` gives it: `BROADCAST_MIN_INTERVAL = 1.0` allows at most one broadcast per second per issuer account. The pair (recipient, `issued_at` at second precision) is therefore unique, and second precision is enough to match a chain transaction.
- History: before PR #138, dividends issued correctly. The code logged an occasional broadcast failure as FAILURE and retried the issuance in the next cycle. PR #138 inserted out-of-band audit rows with rationale `reconciled`, and those rows were duplicates. PR #139 removes them. PR #139 handles "landed on chain, but the DB write failed" without inserting a duplicate row, and improves the matching of chain transactions to DB rows.

**Why:** these invariants decide which settlement and reconciliation designs are valid.

**How to apply:** never write a rationale that the initiating code did not set, and never insert an issuance row from chain data. Treat a second-precision `issued_at` as sufficient for chain matching only while the one-per-second pacing holds. See [[cycle-cadence-and-issuance-volume]].
