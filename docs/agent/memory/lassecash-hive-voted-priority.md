---
name: lassecash-hive-voted-priority
description: L-cash votes go to posts with the SBI Hive vote before posts without it; ruling of 2026-10-25
type: project
---

A post with the SBI Hive vote gets the L-cash vote before every post without the Hive vote, whatever the posts' ages. The lead developer (josephsavage) ruled this on 2026-10-25, during the PR #141 review. `cast_votes` in `hsbi_lassecash_vote.py` therefore reads QUALIFIED rows ordered by `hive_voted DESC, post_created, id` (`vote_rows_with_status` in `hivesbi/lassecash.py`).

**Why:** the job casts one vote per run, and RC limits `sbi-tokens` to a few first votes per 5 days. Oldest-first order let posts without the Hive vote, which become candidates only after 24 hours, use the vote slots before a fresh Hive-voted post. The trade is deliberate: when RC is short, a post without the Hive vote can expire unvoted after its 3-day candidate window.

**How to apply:** keep Hive-voted rows first in any change to the candidate queue or its order. A change that lets posts without the Hive vote compete equally needs a new ruling. See [[lassecash-rc-capacity-operator-managed]].
