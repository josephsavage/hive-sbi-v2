---
name: nectar-fresh-container-config-store
description: make_hive, Hive() and hive-nectar updatenodes fail in a new local app container (nectar 0.2.12 sqlite config store); how to seed it
type: project
---

In a new local container from `docker compose run --rm app`, every `Hive()` construction fails with `sqlite3.ProgrammingError: Error binding parameter 2: type 'list' is not supported`. The error comes from `nectar/storage.py` `generate_config_store`, which writes the node list into an empty `SqliteConfigurationStore`. `hive-nectar updatenodes` fails the same way, so it does not repair the store. Observed on 2026-10-23. Production works because its config store already holds `node`.

**Why:** every job that calls `make_hive` fails in a throwaway container. The failure looks like a code regression, but it is not one.

**How to apply:** for a local smoke run, seed the store first, in the same container:

```sh
python -c "from nectar.storage import SqliteConfigurationStore as S; S()['node']='https://api.hive.blog'"
```

Unit tests do not need the seed, because they never construct `Hive`.
