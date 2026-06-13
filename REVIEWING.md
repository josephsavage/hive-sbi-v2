## Code Review Test Environment

Before reporting dependency/import failures, run tests through Docker Compose:

1. Ensure local config exists: `Copy-Item config.example.json config.json`
2. Start MariaDB: `docker compose up -d mariadb`
3. Run targeted tests: `docker compose run --rm app pytest <paths> -q`

If schema snapshots changed, reset the local DB first:

`docker compose down -v`
`docker compose up -d mariadb`