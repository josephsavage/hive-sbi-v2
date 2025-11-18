#!/bin/bash
hive-nectar updatenodes

python3 -u /root/steembasicincome/hsbi_store_ops_db.py
python3 -u /root/steembasicincome/hsbi_transfer.py
python3 -u /root/steembasicincome/hsbi_check_delegation.py
python3 -u /root/steembasicincome/hsbi_token_snapshot.py
python3 -u /root/steembasicincome/hsbi_claim_rewards.py
python3 -u /root/steembasicincome/hsbi_update_member_db.py
python3 -u /root/steembasicincome/hsbi_store_member_hist.py
python3 -u /root/steembasicincome/hsbi_upvote_post_comment.py
python3 -u /root/steembasicincome/hsbi_stream_post_comment.py
python3 -u /root/steembasicincome/hsbi_manage_accrual.py
#python3 -u /root/steembasicincome/hsbi_reset_rshares.py
