# Test index

pytest discovers `tests/` with default settings. The repository has no `pytest.ini` or `pyproject.toml`.

Run the tests in the app container, so the versions match production:

```powershell
docker compose up -d mariadb
docker compose run --rm app pytest -q

# One file
docker compose run --rm app pytest tests/test_lassecash.py -q
```

The DB-backed tests connect to the local MariaDB (`sbi` through `config.json` `databaseConnector2`). Each one runs inside a transaction that it rolls back. A DB-backed test skips when MariaDB or `config.json` is unavailable. Check `docker compose ps` before a run, and start MariaDB when it is down.

| File | Covers | Needs MariaDB |
|---|---|---|
| `tests/test_memo.py` | `MemoParser` in `hivesbi/memo_parser.py`: sponsor and sponsee extraction from free-text transfer memos, with a mocked `Account` and blockchain. Add a case here for every memo-parsing change. | No |
| `tests/test_virtual_tokens.py` | Delegation `virtual_tokens` (`hsbi_check_delegation.py`); write-ahead token issuance intents; the Management 10% issuance; settlement of unconfirmed issuances (`hsbi_token_snapshot.py`, `hivesbi/issuance_log.py`). | Partly: the pure-logic tests run anywhere. |
| `tests/test_lassecash.py` | The LasseCash companion vote: the Magi client (`hivesbi/magi.py`), candidate, cap, weight and settlement rules (`hivesbi/lassecash.py`), the job and its backfill (`hsbi_lassecash_vote.py`), and the staker refresh hook in `hsbi_token_snapshot.main()`. | Partly: the DB tests also skip when `lassecash_votes` lacks the current columns. |

When you add a test file, add a row to this table.
