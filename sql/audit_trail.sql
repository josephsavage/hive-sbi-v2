--
-- Table structure for table `audit_trail`
--

CREATE TABLE `audit_trail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `account` varchar(50) NOT NULL,
  `value_type` varchar(50) NOT NULL, -- e.g., 'shares', 'bonus_shares', 'curation_rshares', etc.
  `old_value` bigint(22) DEFAULT NULL,
  `new_value` bigint(22) DEFAULT NULL,
  `change_amount` bigint(22) DEFAULT NULL,
  `timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `reason` varchar(255) DEFAULT NULL,
  `related_trx_id` varchar(40) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `account_idx` (`account`),
  KEY `timestamp_idx` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
