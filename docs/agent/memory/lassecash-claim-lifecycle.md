---
name: lassecash-claim-lifecycle
description: After the vote, a LasseCash post needs payout and claim_curation calls on Magi; a follow-up PR to #141 adds them, without settle_pending (ruling of 2026-10-27)
type: project
---

A LasseCash post pays out only when someone calls the contract after the post's window closes. These facts come from the contract source: github.com/LasseEhlers/LasseCash-Migration-to-MAGI at commit d4c29ed (2026-10-20), files `contract/state/pob.go`, `engine/pob.go` and `api/src/aioha-signer.ts`.

- The window closes 7 days (viral) or 30 days (deep) after registration. When SBI registers a post, the window starts at SBI's first vote.
- `payout <author>|<permlink>` is permissionless. It pays the author and parks 25% (`CuratorPct`) of the reward as the curator pot. It refuses "window still open" and "already paid out".
- `claim_curation <author>|<permlink>|[curator]` is permissionless and pays the curator: 20% liquid and 80% pending. It deletes the curator's `pv_` vote record. It refuses "post has not paid out yet" and "no vote to claim".
- Each vote by `sbi-tokens` also claims up to 3 of its queued entries (`PiggybackDrain`), and `settle_pending` claims up to 20. A drain skips a post that has not paid out, and no drain calls `payout`.
- A pending balance becomes a mint only when someone calls `settle_pending` after a month boundary, with at least 1 LASSECASH.
- Anyone can sweep an unclaimed curator pot into the reward pool 365 days after the payout.
- lassecash.com sends `payout` only as side calls on its own users' votes, posts and comments. Its RC limits are 4,000 for `payout` and 1,200 for `claim_curation`.

The lead developer (josephsavage) ruled on 2026-10-27, during the PR #141 review: a follow-up PR adds a `payout` and `claim_curation` phase to `hsbi_lassecash_vote.py`. The job sends no `settle_pending`; the operator sends it.

**Why:** without the phase, a member's post pays out only when a lassecash.com user acts, and the `sbi-tokens` curation share stays parked. PR #141 ships without the phase, because the first payout falls due 7 days after the first live vote.

**How to apply:**

- After a payout, never read a missing `pv_` record as "no vote": `claim_curation` deletes it. The vote job reads `pv_` only inside the voting window, so the deletion cannot reach those reads.
- Dry-run each call. The contract refuses a repeat call, so a lost update costs RC, never a double payment.
- Decide the RC order between votes and claims in the follow-up PR. See [[lassecash-rc-capacity-operator-managed]] and [[live-job-stuck-row-alert]].
