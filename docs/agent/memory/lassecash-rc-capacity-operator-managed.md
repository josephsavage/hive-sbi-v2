---
name: lassecash-rc-capacity-operator-managed
description: sbi-tokens Magi RC is shared with the operator's manual calls; the operator adds HBD on Magi, and the vote job keeps no RC reserve
type: project
---

`sbi-tokens` signs more than the L-cash vote. The operator also signs manual Magi calls from it: swaps, deposits, liquidity and pool claims. All of them draw on the same Magi resource credits (RC). The lead developer (josephsavage) ruled on 2026-10-25, during the PR #141 review, that the operator manages that capacity by adding HBD on Magi. `hsbi_lassecash_vote.py` keeps no RC reserve for the manual calls.

Measured during that review:

- On 2026-10-22 at 00:43:51 UTC, Magi failed the manual deposit in trx `4ceb27945725` with "minimum RC requirement is not met. RCs available: 0".
- On 2026-10-24, a first (registering) vote needed an `rc_limit` of 14,139 to 16,161. Magi reserves that limit for 5 days. Available RC was 93,370.
- On 2026-11-20, the deploy shadow run sized 18,627 RC for `jjprac`, 18,762 for `silvertop` and 18,900 for `elizabethbit`. `size_rc_limit` triples the gas need, so those limits read a `gas_used` near 621 million. The limits run about 15% above the 2026-10-24 range, and `RC_CEILING` (30,000) binds only above a `gas_used` of 1 billion.
- The nine posts queued at that deploy reserve about 169,000 RC together, which exceeds the 2026-10-24 reading of 93,370. The job casts one vote per run, so it logs `RC short` and defers once the reservations exhaust the balance.

**Why:** when the job is live, its reservations and the manual calls compete for one budget. The job defers its own vote when RC is short ("RC short" in `cast_votes`). It does not protect the manual calls.

**How to apply:** when a manual `sbi-tokens` call fails for RC, or the job logs "RC short" often, add HBD on Magi. Read available RC before a deploy turns signing on, and compare it with the queue size times the sized `rc_limit`. Do not add an RC floor to the job unless this ruling changes. See [[lassecash-hive-voted-priority]] and [[lassecash-30-day-volume]].
