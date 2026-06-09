# Hive Stake Based Income

python scripts for automation of Hive Stake Based Income. For full context on functionality, please review our documentation at https://docs.hivesbi.com/

## How to start

## Local development with Docker

The production environment currently runs on Ubuntu 24.04 with Python 3.12.3
and MariaDB 10.11.13. The local Docker setup mirrors those major versions so
Windows can be used as the host/editor without running the Python scripts
natively on Windows.

### First-time setup

Copy the example config and keep the real config uncommitted:

```powershell
Copy-Item config.example.json config.json
```

Build the app image and start MariaDB:

```powershell
docker compose build app
docker compose up -d mariadb
```

The MariaDB container initializes two local databases from the production schema
snapshots in `docker/mariadb/init`:

- `sbi`
- `sbi_steem_ops`

MariaDB is exposed on local port `3307`, with the local development credentials
`sbi` / `sbi`.

### Common commands

Open a shell in the Linux app container:

```powershell
docker compose run --rm app
```

Run tests:

***IMPORTANT***: (For code reviews, do not run `pytest` from the Windows/host Python environment. The host may not have `pytest`, `hive-nectar`, `dataset`, or `mysqlclient`. Use the Docker app container so dependency versions and the MariaDB service match the repo’s expected environment.)

```powershell
docker compose run --rm app pytest
```

Run one script manually:

```powershell
docker compose run --rm app python hsbi_check_member_db.py
```

Reset the local database volume and reload the schema snapshots:

```powershell
docker compose down -v
docker compose up -d mariadb
```

Do not use production Hive keys in the local `config.json`. Scripts that vote,
transfer, claim rewards, issue tokens, or otherwise sign blockchain operations
should be treated as live-operation scripts unless they have been explicitly
made safe for dry-run development.

### Installation of needed packages

The following packages are needed, when running the scripts on Ubuntu:

```bash
apt-get install libmariadbclient-dev
```

```bash
pip3 install hive-nectar dataset mysqlclient
```

Compile and install hivesbi, the helper library for all Hive Stake Based Income scripts

```bash
python setup.py install
```

### Prepare the database

```bash
mysql -u username -p sbi < sql/sbi.sql
mysql -u username -p sbi_steem_ops < sql/sbi_steem_ops.sql
```

### Creating a service script

Main runner script can be automatically run through systemd:

```bash
useradd -r -s /bin/false sbiuser
chown -R sbiuser:sbiuser /etc/sbi

cp systemd/sbirunner.service to /etc/systemd/system/


systemctl enable sbirunner
systemctl start sbirunner

systemctl status sbirunner
```

The blacklist script is run once a day:

```bash

cp systemd/blacklist.service to /etc/systemd/system/
cp systemd/blacklist.timer to /etc/systemd/system/

systemctl enable blacklist.timer
systemctl start blacklist.timer

systemctl list-timers
```

## Config file for accesing the database

A file `config.json` needs to be created:

```json
{

        "databaseConnector": "mysql://user:password@localhost/sbi_steem_ops",
        "databaseConnector2": "mysql://user:password@localhost/sbi",
        "hive_blockchain": true,
        "mgnt_shares": {"josephsavage": 4, "holger80": 1}
}
```

## Running Hive Stake Based Income

The following scripts need to run:

```bash
python3 hsbi_store_ops_db.py
python3 hsbi_transfer.py
python3 hsbi_check_delegation.py
# python3 hsbi_update_curation_rshares.py  # currently disabled in runner
python3 hsbi_manage_accrual.py
python3 hsbi_update_member_db.py
python3 hsbi_store_member_hist.py
python3 hsbi_upvote_post_comment.py
python3 hsbi_stream_post_comment.py
# python3 hsbi_reset_rshares.py  # currently disabled in runner
```

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/josephsavage/hive-sbi-v2)
