---
name: live-job-stuck-row-alert
description: A live job prints an "ALERT:" line for each write-ahead row that stays unsettled past a fixed age; ruling of 2026-10-27
type: project
---

A live job prints a line that starts with `ALERT:` for each write-ahead row that stays unsettled past a fixed age. The lead developer (josephsavage) ruled this on 2026-10-27, during the PR #141 review, for the L-cash vote job. The ruling follows the stuck-issuance alert in `hsbi_token_snapshot.py`.

- `hsbi_token_snapshot.py` prints `ALERT: stuck PENDING issuance` for a PENDING row older than `STUCK_PENDING_AGE` (6 hours).
- `hsbi_lassecash_vote.py` prints `ALERT: stuck PENDING L-cash vote` for a PENDING row older than `STUCK_VOTE_AGE` (6 hours).

**Why:** a stuck write-ahead row blocks later work by design. A held issuance blocks a re-issue, and a PENDING vote blocks every later L-cash vote. Without the alert, the job stops voting and prints only routine lines. The `ALERT:` prefix gives the operator one string to search for.

**How to apply:** give each new write-ahead state in a live job the same alert, with the same prefix. Read the alert from the database alone, so that an outage of an external service cannot hide it. See [[token-issuance-design-invariants]] and [[lassecash-claim-lifecycle]].
