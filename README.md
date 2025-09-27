# Hive Stake Based Income

python scripts for automation of Hive Stake Based Income. For full context on functionality, please review our documentation at https://docs.hivesbi.com/

## How to start

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
