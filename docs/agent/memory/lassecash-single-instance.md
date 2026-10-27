---
name: lassecash-single-instance
description: hsbi_lassecash_vote.py runs only from sbirunner.sh, one instance at a time; ruling of 2026-10-27
type: project
---

`hsbi_lassecash_vote.py` runs only from `sbirunner.sh`, one instance at a time. The lead developer (josephsavage) ruled on 2026-10-27, during the PR #141 review, that nobody runs the job by hand while the runner loops.

The write-ahead in `cast_votes` is a plain `UPDATE ... WHERE id = %s`. It does not claim the row with `AND status = 'QUALIFIED'` and a rowcount check, and the job takes no lock.

**Why:** two concurrent instances can read the same QUALIFIED row, and both see no unsettled vote. Both then broadcast a vote call on one post. The second call replaces the weight, spends vote power again and reserves a second `rc_limit` for 5 days. One instance at a time prevents that race.

**How to apply:** do not run `hsbi_lassecash_vote.py` by hand while `sbirunner` runs, also not at deploy step 4. `--backfill` never signs, so it can run while the runner loops. If a second instance becomes necessary, first make the write-ahead a compare-and-set (`WHERE id = %s AND status = 'QUALIFIED'`, and broadcast only on rowcount 1), or take a job lock. See [[lassecash-hive-voted-priority]].
