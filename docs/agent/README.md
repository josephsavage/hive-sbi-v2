# Agent docs

CLAUDE.md points here. This directory holds working knowledge that the code, CHANGES.md and git history do not record:

- [tests.md](tests.md) is the test index. It has one row per test file: what the file covers, and whether it needs MariaDB.
- [memory/](memory/) is the project memory. It has one file per project fact: a design invariant, a measured volume, or an environment quirk. Each file states why the fact matters and how to apply it.

## Memory index

- [Token issuance design invariants](memory/token-issuance-design-invariants.md): valid issuance rationales, the one-issuance-per-second guarantee, and the PR #138/#139 history.
- [Cycle cadence and issuance volume](memory/cycle-cadence-and-issuance-volume.md): the 144-minute and high-VP (about 15-minute) cycle regimes and the measured issuance peaks.
- [nectar fails in a fresh container](memory/nectar-fresh-container-config-store.md): the `Hive()` sqlite error in a new local app container, and how to seed the node key.
- [L-cash: Hive-voted posts first](memory/lassecash-hive-voted-priority.md): the 2026-10-25 ruling that orders the L-cash candidate queue.
- [L-cash: RC capacity is operator-managed](memory/lassecash-rc-capacity-operator-managed.md): `sbi-tokens` RC is shared with manual Magi calls; the operator adds HBD, and the job keeps no reserve.
- [L-cash: 30-day volume below 42](memory/lassecash-30-day-volume.md): why every weight is 100 now, and when to revisit the weight and the backfill cap.
- [L-cash: one instance at a time](memory/lassecash-single-instance.md): the 2026-10-27 ruling that the vote job runs only from `sbirunner.sh`, and why its write-ahead depends on that.
- [Live jobs: ALERT on a stuck write-ahead row](memory/live-job-stuck-row-alert.md): the 2026-10-27 ruling that a live job prints `ALERT:` for a row stuck past its age.
- [L-cash: payout and claim lifecycle](memory/lassecash-claim-lifecycle.md): the contract's `payout`/`claim_curation` facts, and the 2026-10-27 ruling that a follow-up PR adds them without `settle_pending`.

## Rules

- Add a row to tests.md when you add a test file.
- Write one fact per memory file, with the frontmatter that the existing files use. Add one line for the file to the memory index above.
- Update or delete a memory file when its fact changes. Keep one file per fact.
- Do not record what the code, CLAUDE.md, CHANGES.md or git history already records.
- Write absolute dates, not relative dates.
