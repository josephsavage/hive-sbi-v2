# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

**Hive Stake Based Income (HSBI)** — a Python automation suite for a stake-based
basic-income program on the Hive blockchain. It tracks sponsorships, accrues
"shares" for members, manages voting power, upvotes member posts, and issues the
`HSBIDAO` Hive Engine token. Public docs: https://docs.hivesbi.com/

The repo is two things layered together:

1. **`hivesbi/`** — an installable library (`pip install -e .`, package name
   `hivesbi`) holding all reusable logic: storage wrappers, blockchain history
   parsing, memo parsing, token issuance, and config/runtime bootstrap.
2. **Top-level `hsbi_*.py` scripts** — the runnable jobs. Each is a standalone
   batch task launched in sequence by `sbirunner.sh` (run as a systemd service
   with `Restart=always`, so the whole pipeline loops continuously in prod).

This is a set of cron-style blockchain workers for the backend MariaDB and blockchain interaction. The webserver and API are managed from the separate `*/hive-sbi-unified` repo.



## Environments

- **Production**: Ubuntu 24.04, Python 3.12.3, MariaDB 10.11.13. The systemd
  unit (`systemd/sbirunner.service`) runs `sbirunner.sh` from
  `/root/steembasicincome`.
- **Local dev**: Docker (`Dockerfile.dev` + `docker-compose.yml`) mirrors those
  major versions so Windows can host the editor without running Python natively.
  Pinned prod versions are in `constraints-prod.txt`
  (hive-nectar 0.2.12, dataset 1.6.2, SQLAlchemy 1.4.54, nectarengine 0.2.2).

## Databases

Two MariaDB databases, both `latin1` charset (set in `docker-compose.yml` — keep
it that way; the prod schema is latin1):

- **`sbi`** — the main application DB (members, configuration, accounts, keys,
  transactions, audit trail). Reached via `databaseConnector2` / `databaseConnector3`.
- **`sbi_steem_ops`** — raw blockchain operation history per account. Reached via
  `databaseConnector`.

Local MariaDB is exposed on host port **3307**, credentials **`sbi` / `sbi`**.
Schema snapshots in `docker/mariadb/init/` seed both DBs on first container start.
SQL schema also lives in `sql/` (`sbi.sql`, `sbi_steem_ops.sql`, `audit_trail.sql`).

**Data access uses the `dataset` library, not the SQLAlchemy ORM.** Tables are
accessed dynamically by name (`db[tablename]`), and storage classes in
`hivesbi/storage.py` wrap each table (`TrxDB`, `MemberDB`, `ConfigurationDB`,
`BlacklistDB`, `AccountsDB`, `KeysDB`, `TransactionMemoDB`, `TransactionOutDB`,
`PendingRefundDB`, `AuditDB`, plus `TransferMemoDB`). `transfer_ops_storage.py`
adds `AccountTrx`, `TransferTrx`, `MemberHistDB`, `PostsTrx`,
`CurationOptimizationTrx`. There are no migrations — schema changes go in the
`sql/` files and the `docker/mariadb/init/` snapshots.

## Config

Scripts read `config.json` (gitignored — copy from `config.example.json`). It
holds the DB connectors, `hive_blockchain` flag, account lists, ignore lists,
management shares, and liquidity-pool symbols. Load it through the centralized
loader, never by re-parsing JSON ad hoc:

```python
from hivesbi.settings import get_runtime
rt = get_runtime()          # cfg + db handles + storage objects + conf + accounts
```

`hivesbi/settings.py` is the bootstrap layer: `Config` / `get_config` (cached),
`connect_dbs`, `make_hive`, `make_storages`, and `get_runtime`. Prefer these over
constructing `Hive`, `dataset.connect`, or storage objects by hand.

## Commands

Run everything through the `app` container so versions match prod:

```powershell
# First-time setup
Copy-Item config.example.json config.json
docker compose build app
docker compose up -d mariadb

# Shell, tests, single job
docker compose run --rm app
docker compose run --rm app pytest
docker compose run --rm app python hsbi_check_member_db.py

# Reset local DB volume + reload schema snapshots
docker compose down -v
docker compose up -d mariadb
```

There is no `pytest.ini`/`pyproject.toml` — pytest uses defaults and discovers
`tests/`. Current coverage is `tests/test_memo.py` (memo-parsing cases, mocked
`Account`/blockchain) and `tests/test_virtual_tokens.py`. Add tests alongside it.

DB-backed tests skip if MariaDB/config is unavailable. Check `docker compose ps` for MariaDB availability before running tests and bring containers up if they are not already running.

# Always run review tests inside the app container, not host Python.
Copy-Item config.example.json config.json   # first time only
docker compose build app
docker compose up -d mariadb
docker compose run --rm app pytest -q

# Targeted review run
docker compose run --rm app pytest tests/test_memo.py tests/test_virtual_tokens.py -q

## The pipeline (sbirunner.sh)

Production runs these in order each cycle (see `sbirunner.sh`):

1. `hsbi_store_ops_db.py` — pull new account operations into `sbi_steem_ops`
2. `hsbi_transfer.py` — process incoming sponsorship transfers
3. `hsbi_check_delegation.py` — reconcile delegations; on every delegation change or
   new delegation, zero the delegation trx accrual and grant `tokenholders.virtual_tokens`
   (2× delegated HP) instead of legacy voting-weight bonus shares
4. `hsbi_liquidpools.py` — liquidity-pool handling
5. `hsbi_token_snapshot.py` — snapshot Hive Engine tokenholders; issue per-member PIK
   and Pending-Balance-Conversion dividends (immediate, retried next cycle on failure);
   fail stuck PENDING issuances whose recorded error proves the broadcast never
   reached the chain, and alert on any that carry no such proof; issue the
   Management 10% (write-ahead, capped)
6. `hsbi_claim_rewards.py` — claim HIVE/HBD/VESTS rewards for operator accounts
7. `hsbi_update_member_db.py` — recompute member shares/balances
8. `hsbi_store_member_hist.py` — append member history
9. `hsbi_upvote_post_comment.py` — vote on eligible member posts
10. `hsbi_stream_post_comment.py` — stream new posts/comments into the queue
11. `hsbi_manage_accrual.py` — track fleet mana and scale rshares-per-cycle

`hsbi_update_curation_rshares.py` and `hsbi_reset_rshares.py` exist but are
**disabled** in the runner. The blacklist job runs separately on a daily systemd
timer (`systemd/blacklist.*`). When changing orchestration, update `sbirunner.sh`
and this list together.

## Live-operation safety

Scripts that **vote, transfer, claim rewards, issue tokens, or sign any
blockchain operation are live** (`hsbi_upvote_post_comment.py`, `hsbi_transfer.py`,
`hsbi_claim_rewards.py`, `hsbi_token_snapshot.py`, anything using `make_hive` with
keys, and `hivesbi/issue.py`). **Never put production Hive keys in a local
`config.json`**, and assume an unfamiliar `hsbi_*` script can broadcast unless
proven otherwise. Token issuance goes through `hivesbi/issue.py` (`TokenIssuer`,
`HSBIDAO` symbol, `hivesbi` issuer account) and is logged to `token_issuance_log`
for on-chain/off-chain reconciliation.

## History & memo parsing

`hivesbi/parse_hist_op.py` (`ParseAccountHist`) walks an account's operation
history, classifies transfers, and drives share/token bookkeeping. Sponsorship
intent comes from transfer memos parsed by `hivesbi/memo_parser.py` (`MemoParser`)
— it extracts sponsor/sponsee from free-text memos against an allow-list of
filler words. Memo parsing is the most test-covered and edge-case-prone area;
when touching it, add cases to `tests/test_memo.py`.

## Git

- Remote: `josephsavage/hive-sbi-v2` (private). Author: josephsavage.
- `gh` CLI is installed and authenticated as `josephsavage`, so PRs can be opened
  directly (`gh pr create --base main`). Branch protection on `main` reports
  `mergeable_state: blocked` until review/checks pass — that is not a conflict.
- Confirm before pushing. Don't push directly to the main branch; use feature
  branches.
- **PRs are squash-merged.** A local feature branch therefore keeps its
  pre-squash commits and looks "N commits ahead" of `main` forever, even after it
  has landed. Commit counts prove nothing here; compare trees before assuming
  work is unmerged:

  ```sh
  git diff --stat <branch> origin/main   # empty output = already in main
  ```

  When starting fresh work, branch from `origin/main`, not from a stale local
  branch, or the PR replays already-merged history.

## Reference

`CHANGES.md` is a running log of recent behavioral changes (token issuance, mana
tracking/accrual scaling, voting eligibility/throttling) — read it to understand
the most recent intent before editing those areas. Ask before refactoring.
