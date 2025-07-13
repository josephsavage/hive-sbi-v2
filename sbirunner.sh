#!/bin/bash
hive-nectar updatenodes --hive

python3 -u /root/steembasicincome/sbi_store_ops_db.py
python3 -u /root/steembasicincome/sbi_transfer.py
python3 -u /root/steembasicincome/sbi_check_delegation.py
#python3 -u /root/steembasicincome/sbi_update_curation_rshares.py
python3 -u /root/steembasicincome/sbi_update_member_db.py

python3 -u /root/steembasicincome/sbi_store_member_hist.py
python3 -u /root/steembasicincome/sbi_upvote_post_comment.py

python3 -u /root/steembasicincome/sbi_stream_post_comment.py

python3 -u /root/steembasicincome/sbi_manage_accrual.py

#python3 -u /root/steembasicincome/sbi_reset_rshares.py
