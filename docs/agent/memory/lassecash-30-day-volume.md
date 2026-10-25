---
name: lassecash-30-day-volume
description: The LasseCash 30-day history holds fewer than 42 posts (2026-10-25), so every L-cash vote weight is 100 and the backfill simplification changes no weight
type: project
---

The LasseCash 30-day history holds fewer than 42 posts. The lead developer (josephsavage) stated this on 2026-10-25, during the PR #141 review. The PR #141 body measured 27 vote calls on 18 posts by 7 authors in the 30 days before 2026-10-23.

`compute_weight` in `hivesbi/lassecash.py` returns 100 while the charged votes in the lookback, the new vote included, are 42 or fewer. At this volume every L-cash vote therefore goes out at 100%.

**Why:** two simplifications are safe only below 42 votes per 30 days:

- `--backfill` (`apply_cap`) drops a capped post, but the live job defers it and votes it later. The backfill therefore counts fewer votes than the live job casts.
- The weight formula spreads the 30-day meter budget over a vote count, not over the weights already spent.

**How to apply:** when the charged votes approach 42 per 30 days, check the weight against the meter again. Make `apply_cap` defer a capped post to the end of its cap inside its candidate window, as the live job does. See [[lassecash-rc-capacity-operator-managed]].
