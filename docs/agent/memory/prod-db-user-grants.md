---
name: prod-db-user-grants
description: the prod DB user sbi2@localhost holds limited privileges, so a new table needs an explicit GRANT at deploy; observed 2026-11-20
type: project
---

The production application connects to MariaDB as `sbi2`@`localhost`, and that user does not hold every privilege on the `sbi` database. On 2026-11-20 it held SELECT, INSERT and UPDATE on the new `lassecash_stakers` table and no DELETE, so deploy step 4 of the LasseCash companion vote failed with MySQLdb error 1142, DELETE command denied. Local dev differs: `docker/mariadb/init/00-create-databases.sql` grants ALL PRIVILEGES to the `sbi` user, so no job hits this limit in the app container.

No job in `sbirunner.sh` deleted a row from the `sbi` database before that deploy. Every `delete` method in `hivesbi/storage.py` has its only call site in `hsbi_build_member_db.py`, which the runner does not run. The DELETE privilege was therefore never exercised in prod.

**Why:** a schema change that passes every local test still fails in prod when the new table needs a privilege that `sbi2` lacks. The failure surfaces as a `sqlalchemy.exc.OperationalError` at run time, in the first cycle after deploy, not at schema-apply time.

**How to apply:** when a change adds a table to the `sbi` schema, add a GRANT step to the deploy runbook in CHANGES.md next to the CREATE TABLE step. Grant only the statements the code issues against that table. Read `SHOW GRANTS FOR 'sbi2'@'localhost';` first: a grant on `sbi`.* already covers a new table for the privileges it lists. When a new code path issues a statement kind that the table's existing grant omits, such as the first DELETE against a table, the deploy needs a grant too. See [[lassecash-single-instance]].
