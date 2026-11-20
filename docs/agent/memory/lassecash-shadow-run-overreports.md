---
name: lassecash-shadow-run-overreports
description: a broadcast-off shadow run of hsbi_lassecash_vote.py reports every QUALIFIED post each cycle, because it applies neither MAX_VOTES_PER_RUN nor the 24-hour author cap
type: reference
---

A shadow run of `hsbi_lassecash_vote.py` (`lassecash.broadcast` false) prints one `shadow run, would vote` line per `QUALIFIED` row, every cycle. It reports more votes than the live job casts, for two reasons in `cast_votes`:

- `cast_votes` increments its `cast` counter only after a broadcast, so the `MAX_VOTES_PER_RUN = 1` check stops nothing in a shadow run. The live job casts one vote per run.
- The 24-hour author cap reads `last_attempt`, which is `MAX(broadcast_at)` for the author. A shadow run writes no `broadcast_at`, so `last_attempt` returns None and the cap defers nothing. The live job defers the author's later posts for 24 hours, unless the row carries `hive_voted = 1`, which bypasses the cap.

On 2026-11-20 the deploy shadow run reported nine posts per cycle, three each for `jjprac`, `elizabethbit` and `silvertop`, at 46% weight and `rc_limit` 18,627 to 18,900.

**Why:** an operator reading nine shadow lines can conclude that step 6 casts nine votes in one cycle. The live job casts at most one per run, and the cap spreads an author's posts across days. The shadow run is therefore a per-post weight and `rc_limit` preview, not a schedule.

**How to apply:** read a shadow line as "this post qualifies, at this weight and this `rc_limit`". To project what the live job casts, read `hive_voted` on the `QUALIFIED` rows: rows with `hive_voted = 1` go out one per run, and rows with `hive_voted = 0` go out one per author per 24 hours. Multiply the row count by the sized `rc_limit` before step 6, and compare the product with available RC. See [[lassecash-rc-capacity-operator-managed]], [[lassecash-hive-voted-priority]] and [[lassecash-30-day-volume]].
