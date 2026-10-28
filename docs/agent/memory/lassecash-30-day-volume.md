---
name: lassecash-30-day-volume
description: The LasseCash 30-day history holds fewer than 42 posts (2026-10-25); L-cash weights stay 100 up to 42 viral or 10 deep charged votes, and the deep vote count is unmeasured
type: project
---

The LasseCash 30-day history holds fewer than 42 posts. The lead developer (josephsavage) stated this on 2026-10-25, during the PR #141 review. The PR #141 body measured 27 vote calls on 18 posts by 7 authors in the 30 days before 2026-10-23.

`compute_weight` in `hivesbi/lassecash.py` divides one meter's 30-day budget by the charged votes in that meter's lookback, the new vote included. The weight stays 100 up to a threshold that depends on the window:

- Viral meter: budget 4,285. The weight is 100 up to 42 charged votes.
- Deep meter: budget 1,000. The weight is 100 up to 10 charged votes, and 90 at 11 votes.

At this volume every viral L-cash vote therefore goes out at 100%. A deep vote goes out at 100% only while the deep meter holds 10 or fewer charged votes. The number of deep-window votes SBI casts per 30 days is unverified: the 2026-10-23 measurement does not split the calls by window.

**Why:** two simplifications are safe only while each meter stays at or below its threshold:

- `--backfill` (`apply_cap`) drops a capped post, but the live job defers it and votes it later. The backfill therefore counts fewer votes than the live job casts.
- The weight formula spreads the 30-day meter budget over a vote count, not over the weights already spent.

**How to apply:** count the charged votes per meter, not in total. When the viral votes approach 42 or the deep votes approach 10 per 30 days, check the weight against the meter again. Make `apply_cap` defer a capped post to the end of its cap inside its candidate window, as the live job does. See [[lassecash-rc-capacity-operator-managed]].
