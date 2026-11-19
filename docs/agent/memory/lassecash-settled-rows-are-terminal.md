---
name: lassecash-settled-rows-are-terminal
description: A settled lassecash_votes row is terminal: the job never retries a FAILED vote and never reclassifies an IGNORED post; rulings of 2026-11-19
type: project
---

A settled `lassecash_votes` row is terminal. `hsbi_lassecash_vote.py` reads only `PENDING` and `BROADCAST` rows in `settle_votes`, and only `QUALIFIED` rows in `cast_votes`. The lead developer (josephsavage) ruled on both consequences on 2026-11-19, during the PR #141 review.

- **A vote that Magi aborts stays `FAILED`.** The job never retries the post, and the post gets no L-cash vote in its remaining candidate window. The author's 24-hour cap still counts the attempt, because `last_attempt` reads `MAX(broadcast_at)` over every status. `meter_votes` still excludes the row, because the abort spent no vote power. The two queries diverge on purpose.
- **A post recorded `IGNORED` stays `IGNORED`.** The job qualifies a post once, and a later edit that adds the `lassecash` tag changes nothing.

**Why:** the value a single post carries is small, and covering these two cases costs more than the votes they lose. The ruling trades completeness for a job that revisits no settled row. One known gap follows from it: when Magi applies a Hive-included call more than 30 minutes after the broadcast, `settle_votes` has already written `FAILED`, and no run re-reads the row against the `pv_` key. The deep meter then undercounts by one vote, so a later deep vote can go out at up to 10 weight-percent too high. The contract's own vote-power check defers such a vote rather than overspending the meter. Unverified: Magi's apply latency during an indexer lag.

**How to apply:**

- Do not add a retry path for `FAILED` or a re-check for `IGNORED` without a new ruling.
- Read a `FAILED` row as "no vote exists on Magi" only for the author's cap, never for the meter.
- Revisit the `pv_` re-read if the deep-window vote count per 30 days ever approaches 10. See [[lassecash-30-day-volume]] and [[lassecash-single-instance]].
