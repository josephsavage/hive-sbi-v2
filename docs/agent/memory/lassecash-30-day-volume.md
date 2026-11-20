---
name: lassecash-30-day-volume
description: the 2026-11-20 prod backfill charged 92 votes to the viral meter and 3 to the deep meter, so L-cash viral weights run near 46, not 100
type: project
---

The deploy backfill of 2026-11-20 measured the SBI-side volume on prod: 11 eligible stakers, 240 member root posts in 30 days, 100 posts carrying the `lassecash` tag, 95 of them counted (`BASELINE`) and 5 capped (`SKIPPED`). Four accounts (`beelshops`, `elizabethbit`, `jjprac`, `silvertop`) wrote 93 of the 100 tagged posts as Actifit activity reports, at about one post per account per day. The LasseCash contract had registered a vote on only 7 of the 100 posts, all by `lasseehlers`.

`compute_weight` in `hivesbi/lassecash.py` divides one meter's 30-day budget by the charged votes in that meter's lookback, the new vote included. `post_window` records an unregistered post as viral. Of the 95 `BASELINE` rows, 92 charge the viral meter and 3 charge the deep meter.

- Viral meter: budget 4,285. The weight is 100 up to 42 charged votes, 46 at 93 votes, 35 at 121 votes.
- Deep meter: budget 1,000. The weight is 100 up to 10 charged votes, and 90 at 11 votes. At 3 charged votes a deep vote still goes out at 100.

The viral count therefore starts above the 42-vote threshold on the first live cycle. The shadow run of 2026-11-20 21:03 UTC confirms the consequence: it reports 46% at 93 viral votes for every one of the nine queued posts. The four Actifit accounts keep the count near 95 to 120 per 30 days while they post daily, so the weight settles between 35 and 46. Read the split per meter with `SELECT status, vote_window, COUNT(*) FROM lassecash_votes GROUP BY status, vote_window;`.

This measurement supersedes the estimate of 2026-10-25, when the lead developer (josephsavage) stated that the LasseCash 30-day history holds fewer than 42 posts. That estimate counted the contract's own vote calls (27 calls on 18 posts by 7 authors in the 30 days before 2026-10-23). The SBI-side count is larger, because SBI votes its members' tagged posts whether or not the contract already holds a vote on them.

**Why:** two simplifications are safe only while each meter stays at or below its threshold, and the viral meter no longer does:

- `--backfill` (`apply_cap`) drops a capped post, but the live job defers it and votes it later. The backfill therefore counts fewer votes than the live job casts, and above the threshold that difference changes the weight.
- The weight formula spreads the 30-day meter budget over a vote count, not over the weights already spent. The backfill seeds the lookback full, which keeps the count from ramping up from 1 and overspending the meter.

**How to apply:** expect a viral weight near 46, not 100, in the shadow-run and live log lines. Read the count per meter, not in total. Make `apply_cap` defer a capped post to the end of its cap inside its candidate window, as the live job does, before trusting the weight to track the meter. Decide with the operator whether Actifit activity reports should draw on the LasseCash pool: they carry the `lassecash` tag, so the tag rule accepts them, and they hold 93 of the 100 tagged posts. See [[lassecash-rc-capacity-operator-managed]] and [[lassecash-single-instance]].
