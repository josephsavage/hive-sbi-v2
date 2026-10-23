# Agent docs

CLAUDE.md points here. This directory holds working knowledge that the code, CHANGES.md and git history do not record:

- [tests.md](tests.md) is the test index. It has one row per test file: what the file covers, and whether it needs MariaDB.
- [memory/](memory/) is the project memory. It has one file per project fact: a design invariant, a measured volume, or an environment quirk. Each file states why the fact matters and how to apply it.

## Memory index

- [Token issuance design invariants](memory/token-issuance-design-invariants.md): valid issuance rationales, the one-issuance-per-second guarantee, and the PR #138/#139 history.
- [Cycle cadence and issuance volume](memory/cycle-cadence-and-issuance-volume.md): the 144-minute and high-VP (about 15-minute) cycle regimes and the measured issuance peaks.
- [nectar fails in a fresh container](memory/nectar-fresh-container-config-store.md): the `Hive()` sqlite error in a new local app container, and how to seed the node key.

## Rules

- Add a row to tests.md when you add a test file.
- Write one fact per memory file, with the frontmatter that the existing files use. Add one line for the file to the memory index above.
- Update or delete a memory file when its fact changes. Keep one file per fact.
- Do not record what the code, CLAUDE.md, CHANGES.md or git history already records.
- Write absolute dates, not relative dates.
