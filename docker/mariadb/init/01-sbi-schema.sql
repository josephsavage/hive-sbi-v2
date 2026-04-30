/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19  Distrib 10.11.13-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: sbi
-- ------------------------------------------------------
-- Server version	10.11.13-MariaDB-0ubuntu0.24.04.1-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

USE `sbi`;

--
-- Table structure for table `accounts`
--

DROP TABLE IF EXISTS `accounts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `accounts` (
  `name` varchar(16) NOT NULL,
  `voting` tinyint(1) NOT NULL,
  `transfer` tinyint(1) NOT NULL DEFAULT 0,
  `upvote_reward_rshares` tinyint(1) NOT NULL DEFAULT 0,
  `transfer_memo_sender` tinyint(1) NOT NULL DEFAULT 0,
  `last_paid_post` datetime DEFAULT NULL,
  `last_paid_comment` datetime DEFAULT NULL,
  `enrollments` tinyint(4) DEFAULT 0,
  `current_mana` bigint(20) DEFAULT NULL,
  `max_mana` bigint(20) DEFAULT NULL,
  `mana_pct` decimal(7,4) DEFAULT NULL,
  `last_checked` timestamp NULL DEFAULT NULL,
  `reward_hive` varchar(65) DEFAULT '0.000000',
  `reward_hbd` varchar(65) DEFAULT '0.000000',
  `reward_vests` varchar(65) DEFAULT '0.000000000000',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `audit_rshares_trim`
--

DROP TABLE IF EXISTS `audit_rshares_trim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `audit_rshares_trim` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `account_name` varchar(255) NOT NULL,
  `old_rshares` bigint(22) NOT NULL,
  `reason` varchar(255) NOT NULL,
  `batch_id` bigint(20) unsigned NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `new_rshares` bigint(22) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `idx_account_name` (`account_name`),
  KEY `idx_batch_id` (`batch_id`),
  KEY `idx_reason` (`reason`)
) ENGINE=InnoDB AUTO_INCREMENT=124113 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `audit_trail`
--

DROP TABLE IF EXISTS `audit_trail`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `audit_trail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `account` varchar(50) NOT NULL,
  `value_type` varchar(50) NOT NULL,
  `old_value` bigint(22) DEFAULT NULL,
  `new_value` bigint(22) DEFAULT NULL,
  `change_amount` bigint(22) DEFAULT NULL,
  `timestamp` datetime NOT NULL DEFAULT current_timestamp(),
  `reason` varchar(255) DEFAULT NULL,
  `related_trx_id` varchar(40) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `account_idx` (`account`),
  KEY `timestamp_idx` (`timestamp`)
) ENGINE=InnoDB AUTO_INCREMENT=959 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `balance_conversion`
--

DROP TABLE IF EXISTS `balance_conversion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `balance_conversion` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `account` varchar(255) NOT NULL,
  `total_units` bigint(20) NOT NULL DEFAULT 0,
  `balance_rshares` bigint(20) NOT NULL,
  `trimmed_rshares` bigint(20) NOT NULL,
  `vests` decimal(36,6) NOT NULL,
  `member_vests` decimal(36,6) NOT NULL DEFAULT 0.000000,
  `pik_prospective` decimal(36,6) NOT NULL DEFAULT 0.000000,
  `conversion_date` datetime NOT NULL DEFAULT current_timestamp(),
  `balance_hbd` decimal(10,3) DEFAULT 0.000,
  `conversion_hbd` decimal(10,3) DEFAULT 0.000,
  `batch_id` bigint(20) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `idx_account` (`account`),
  KEY `idx_conversion_date` (`conversion_date`)
) ENGINE=InnoDB AUTO_INCREMENT=125245 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `batch_seq`
--

DROP TABLE IF EXISTS `batch_seq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `batch_seq` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1481 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `blacklist`
--

DROP TABLE IF EXISTS `blacklist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `blacklist` (
  `id` int(11) NOT NULL,
  `tags` text NOT NULL,
  `apps` text NOT NULL,
  `body` text NOT NULL,
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `configuration`
--

DROP TABLE IF EXISTS `configuration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `configuration` (
  `id` enum('1') NOT NULL,
  `share_cycle_min` float NOT NULL,
  `sp_share_ratio` float NOT NULL,
  `rshares_per_cycle` bigint(20) DEFAULT 800000000,
  `del_rshares_per_cycle` bigint(20) NOT NULL DEFAULT 80000000,
  `comment_vote_divider` float DEFAULT NULL,
  `comment_vote_timeout_h` float DEFAULT NULL,
  `last_cycle` datetime DEFAULT NULL,
  `upvote_multiplier` float NOT NULL DEFAULT 1.05,
  `upvote_multiplier_adjusted` float NOT NULL DEFAULT 1,
  `last_paid_post` datetime DEFAULT NULL,
  `last_paid_comment` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `minimum_vote_threshold` bigint(20) NOT NULL DEFAULT 800000000,
  `last_delegation_check` datetime DEFAULT NULL,
  `comment_footer` text NOT NULL,
  `mana_pct_target` int(11) DEFAULT 45,
  `max_mana` bigint(20) DEFAULT 1486285070565004,
  `mana_threshold` decimal(5,2) DEFAULT 1.05,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dividend_batches`
--

DROP TABLE IF EXISTS `dividend_batches`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `dividend_batches` (
  `batch_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `run_timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `divs_per` decimal(20,8) NOT NULL,
  `vests_per_unit` decimal(20,8) NOT NULL,
  PRIMARY KEY (`batch_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4663 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `member`
--

DROP TABLE IF EXISTS `member`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `member` (
  `account` varchar(50) NOT NULL,
  `note` text DEFAULT NULL,
  `shares` int(11) NOT NULL,
  `bonus_shares` int(11) NOT NULL,
  `total_share_days` int(11) DEFAULT NULL,
  `avg_share_age` float DEFAULT NULL,
  `last_comment` datetime DEFAULT NULL,
  `last_post` datetime DEFAULT NULL,
  `original_enrollment` datetime DEFAULT NULL,
  `latest_enrollment` datetime DEFAULT NULL,
  `flags` text DEFAULT NULL,
  `earned_rshares` bigint(22) DEFAULT NULL,
  `subscribed_rshares` bigint(20) NOT NULL DEFAULT 0,
  `curation_rshares` bigint(20) NOT NULL DEFAULT 0,
  `delegation_rshares` bigint(20) NOT NULL DEFAULT 0,
  `other_rshares` bigint(20) NOT NULL DEFAULT 0,
  `rewarded_rshares` bigint(22) DEFAULT NULL,
  `balance_rshares` bigint(22) DEFAULT NULL,
  `upvote_delay` float DEFAULT NULL,
  `comment_upvote` tinyint(1) DEFAULT NULL,
  `updated_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `first_cycle_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `hivewatchers` tinyint(1) DEFAULT NULL,
  `buildawhale` tinyint(1) DEFAULT NULL,
  `blacklisted` tinyint(1) DEFAULT NULL,
  `last_received_vote` datetime DEFAULT NULL,
  `vote_ready` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`account`),
  KEY `ix_member_ca20728d35f00c3a` (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `member_backup`
--

DROP TABLE IF EXISTS `member_backup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `member_backup` (
  `account` varchar(50) NOT NULL,
  `note` text DEFAULT NULL,
  `shares` int(11) NOT NULL,
  `bonus_shares` int(11) NOT NULL,
  `total_share_days` int(11) DEFAULT NULL,
  `avg_share_age` float DEFAULT NULL,
  `last_comment` datetime DEFAULT NULL,
  `last_post` datetime DEFAULT NULL,
  `original_enrollment` datetime DEFAULT NULL,
  `latest_enrollment` datetime DEFAULT NULL,
  `flags` text DEFAULT NULL,
  `earned_rshares` bigint(22) DEFAULT NULL,
  `subscribed_rshares` bigint(20) NOT NULL DEFAULT 0,
  `curation_rshares` bigint(20) NOT NULL DEFAULT 0,
  `delegation_rshares` bigint(20) NOT NULL DEFAULT 0,
  `other_rshares` bigint(20) NOT NULL DEFAULT 0,
  `rewarded_rshares` bigint(22) DEFAULT NULL,
  `balance_rshares` bigint(22) DEFAULT NULL,
  `upvote_delay` float DEFAULT NULL,
  `comment_upvote` tinyint(1) DEFAULT NULL,
  `post_hist_28` int(11) DEFAULT NULL,
  `post_hist_7` int(11) DEFAULT NULL,
  `norm_post_hist_7` float DEFAULT NULL,
  `upvote_count_28` int(11) DEFAULT NULL,
  `upvote_weight_28` float DEFAULT NULL,
  `updated_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `first_cycle_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `steemcleaners` tinyint(1) DEFAULT NULL,
  `buildawhale` tinyint(1) DEFAULT NULL,
  `blacklisted` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`account`),
  KEY `ix_member_ca20728d35f00c3a` (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `member_hist`
--

DROP TABLE IF EXISTS `member_hist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `member_hist` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pending_refunds`
--

DROP TABLE IF EXISTS `pending_refunds`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `pending_refunds` (
  `index` int(11) DEFAULT NULL,
  `sender` varchar(16) NOT NULL,
  `to` varchar(16) DEFAULT NULL,
  `memo` text DEFAULT NULL,
  `encrypted` tinyint(1) DEFAULT 0,
  `referenced_accounts` text DEFAULT NULL,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `steem_keys`
--

DROP TABLE IF EXISTS `steem_keys`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `steem_keys` (
  `SBI_account_PK` tinyint(4) NOT NULL,
  `account` varchar(50) NOT NULL,
  `key_type` varchar(64) NOT NULL,
  `wif` varchar(64) NOT NULL,
  PRIMARY KEY (`SBI_account_PK`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `token_issuance_log`
--

DROP TABLE IF EXISTS `token_issuance_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `token_issuance_log` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `trx_id` varchar(100) NOT NULL,
  `recipient` varchar(100) NOT NULL,
  `units` decimal(16,3) NOT NULL,
  `issued_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `status` enum('SUCCESS','FAILURE') NOT NULL,
  `error_message` text DEFAULT NULL,
  `rationale` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_recipient` (`recipient`),
  KEY `idx_trx_id` (`trx_id`)
) ENGINE=InnoDB AUTO_INCREMENT=159353 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tokenholders`
--

DROP TABLE IF EXISTS `tokenholders`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `tokenholders` (
  `snapshot_timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `member_name` varchar(64) NOT NULL,
  `liquid_tokens` decimal(15,3) NOT NULL DEFAULT 0.000,
  `accrued_dividends` decimal(36,3) DEFAULT 0.000,
  `pik` decimal(15,3) DEFAULT 0.000,
  `last_batch_id` bigint(20) DEFAULT NULL,
  `abc_vests` decimal(36,3) DEFAULT 0.000,
  `abc_pik` decimal(15,3) DEFAULT 0.000,
  `LP_tokens` decimal(15,3) NOT NULL DEFAULT 0.000,
  `tokens` decimal(15,3) GENERATED ALWAYS AS (`liquid_tokens` + `LP_tokens`) VIRTUAL,
  PRIMARY KEY (`member_name`),
  UNIQUE KEY `uq_member_name` (`member_name`),
  KEY `fk_tokenholders_batch` (`last_batch_id`),
  CONSTRAINT `fk_tokenholders_batch` FOREIGN KEY (`last_batch_id`) REFERENCES `dividend_batches` (`batch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transaction_memo`
--

DROP TABLE IF EXISTS `transaction_memo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `transaction_memo` (
  `index` int(11) DEFAULT NULL,
  `sender` varchar(16) NOT NULL,
  `to` varchar(16) DEFAULT NULL,
  `memo` text DEFAULT NULL,
  `encrypted` tinyint(1) DEFAULT 0,
  `referenced_accounts` text DEFAULT NULL,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=348265437 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transaction_out`
--

DROP TABLE IF EXISTS `transaction_out`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `transaction_out` (
  `index` int(11) DEFAULT NULL,
  `sender` varchar(16) NOT NULL,
  `to` varchar(16) DEFAULT NULL,
  `memo` text DEFAULT NULL,
  `encrypted` tinyint(1) DEFAULT 0,
  `referenced_accounts` text DEFAULT NULL,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6597852 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transfer_memos`
--

DROP TABLE IF EXISTS `transfer_memos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `transfer_memos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `memo_type` varchar(50) NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT 0,
  `memo` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `trx`
--

DROP TABLE IF EXISTS `trx`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `trx` (
  `index` int(11) NOT NULL,
  `source` varchar(50) NOT NULL,
  `memo` text DEFAULT NULL,
  `account` varchar(50) DEFAULT NULL,
  `sponsor` varchar(50) DEFAULT NULL,
  `sponsee` text DEFAULT NULL,
  `shares` int(11) DEFAULT NULL,
  `vests` decimal(15,6) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `status` varchar(50) NOT NULL,
  `share_type` varchar(50) NOT NULL,
  PRIMARY KEY (`index`,`source`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `trx_backup`
--

DROP TABLE IF EXISTS `trx_backup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `trx_backup` (
  `index` int(11) NOT NULL,
  `source` varchar(50) NOT NULL,
  `memo` text DEFAULT NULL,
  `account` varchar(50) DEFAULT NULL,
  `sponsor` varchar(50) DEFAULT NULL,
  `sponsee` text DEFAULT NULL,
  `shares` int(11) DEFAULT NULL,
  `vests` decimal(15,6) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `status` varchar(50) NOT NULL,
  `share_type` varchar(50) NOT NULL,
  PRIMARY KEY (`index`,`source`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `vw_member_status`
--

DROP TABLE IF EXISTS `vw_member_status`;
/*!50001 DROP VIEW IF EXISTS `vw_member_status`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_member_status` AS SELECT
 1 AS `account`,
  1 AS `balance_rshares`,
  1 AS `skiplist`,
  1 AS `vote_ready` */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vw_metrics`
--

DROP TABLE IF EXISTS `vw_metrics`;
/*!50001 DROP VIEW IF EXISTS `vw_metrics`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_metrics` AS SELECT
 1 AS `total_units`,
  1 AS `tokens`,
  1 AS `total_mana`,
  1 AS `reward_vests`,
  1 AS `mana_per_unit`,
  1 AS `vests_per_unit`,
  1 AS `token_ratio`,
  1 AS `divs`,
  1 AS `divs_per`,
  1 AS `accrual`,
  1 AS `min_vote`,
  1 AS `max_vote`,
  1 AS `max_multiplier`,
  1 AS `max_vote_value`,
  1 AS `implied_apr` */;
SET character_set_client = @saved_cs_client;

--
-- Dumping routines for database 'sbi'
--
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
/*!50003 DROP PROCEDURE IF EXISTS `usp_curation_dividends` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
DELIMITER ;;
CREATE DEFINER=`sbi`@`%` PROCEDURE `usp_curation_dividends`()
BEGIN
    DECLARE v_divs_per DECIMAL(20,8);
    DECLARE v_vests_per_unit DECIMAL(20,8);
    DECLARE v_batch_id BIGINT;

    -- Step 1: Get current divs_per and vests_per_unit
    SELECT divs_per, vests_per_unit
    INTO v_divs_per, v_vests_per_unit
    FROM vw_metrics
    LIMIT 1;

    -- Step 2: Record this run in audit trail
    INSERT INTO dividend_batches (divs_per, vests_per_unit)
    VALUES (v_divs_per, v_vests_per_unit);

    -- Capture the new batch_id
    SET v_batch_id = LAST_INSERT_ID();

    -- Step 3: Update accrued_dividends for each tokenholder (excluding sbi-tokens)
    UPDATE tokenholders
    SET accrued_dividends = accrued_dividends + (tokens * v_divs_per),
        last_batch_id = v_batch_id
    WHERE member_name <> 'sbi-tokens';

    -- Step 4: Multi-unit aware adjustment with fractional issuance (0.01 pik)
	UPDATE tokenholders
	SET pik = pik + FLOOR(accrued_dividends / (v_vests_per_unit / 100)) * 0.01,
    	accrued_dividends = MOD(accrued_dividends, (v_vests_per_unit / 100)),
    	last_batch_id = v_batch_id
	WHERE accrued_dividends >= (v_vests_per_unit / 100)
	  AND member_name <> 'sbi-tokens';
    
    -- Step 5: Fractional issuance for abc_vests into abc_pik
	UPDATE sbi.tokenholders
	SET abc_pik = abc_pik + FLOOR(abc_vests / (v_vests_per_unit / 100)) * 0.01,
    	abc_vests = MOD(abc_vests, (v_vests_per_unit / 100)),
    	last_batch_id = v_batch_id
	WHERE abc_vests >= (v_vests_per_unit / 100)
	  AND member_name <> 'sbi-tokens';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
/*!50003 DROP PROCEDURE IF EXISTS `usp_member_accrual_cutoff` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
DELIMITER ;;
CREATE DEFINER=`sbi`@`%` PROCEDURE `usp_member_accrual_cutoff`()
BEGIN    
	DECLARE cutoff BIGINT;
    DECLARE trimmed BIGINT;
    DECLARE vests DECIMAL(20,6);
	DECLARE max_votes DECIMAL(20,6);

-- Establish batch ID (could be a UUID, sequence, or timestamp)
	INSERT INTO sbi.batch_seq VALUES ();   -- generates a new row
	SET @batch_id = LAST_INSERT_ID();      -- captures the new batch ID

-- Trim 0 unit members down to 0
	INSERT INTO audit_rshares_trim (
    	account_name,
    	old_rshares,
    	new_rshares,
    	reason,
    	batch_id
	)
	SELECT m.account,
   		m.balance_rshares,
       	0,
       	'below_min_vote_threshold_and_zero_shares',
       	@batch_id
	FROM member AS m
	JOIN sbi.vw_metrics AS vw ON 1 = 1
	WHERE (m.shares + m.bonus_shares) = 0
	  AND m.balance_rshares < (vw.min_vote * 3)
		AND m.balance_rshares <> 0;

	UPDATE member AS m
	JOIN sbi.vw_metrics AS vw
	  ON 1 = 1
	SET m.balance_rshares = 0
	WHERE m.balance_rshares < (vw.min_vote * 3) and (m.shares + m.bonus_shares) = 0 and m.balance_rshares <> 0;

-- Capture trimmed values into conversion table
    INSERT INTO sbi.balance_conversion (
        account,
        total_units,
        balance_rshares,
        balance_hbd,
        trimmed_rshares,
        conversion_hbd,
        vests,
        pik_prospective,
        member_vests,
        conversion_date,
        batch_id
    )
    SELECT m.account,
    	   (m.shares + m.bonus_shares) AS total_units,
    	   (m.balance_rshares) as balance_rshares,
    	   ((m.balance_rshares / vw.min_vote) * 0.021) as balance_hbd,
           ((m.balance_rshares - LEAST((m.shares + m.bonus_shares), 10) * vw.max_vote) * 0.0150) AS trimmed,
           ((((m.balance_rshares - LEAST((m.shares + m.bonus_shares), 10) * vw.max_vote) * 0.0150) / vw.min_vote) * 0.021) AS conversion_hbd,
		   (((m.balance_rshares - LEAST((m.shares + m.bonus_shares), 10) * vw.max_vote) * 0.0150) / 1000000.0) AS vests,
           ((((m.balance_rshares - LEAST((m.shares + m.bonus_shares), 10) * vw.max_vote) * 0.0150) / 1000000.0) / vw.total_units) as pik_prospective,
           (((((m.balance_rshares - LEAST((m.shares + m.bonus_shares), 10) * vw.max_vote) * 0.0150) / 1000000.0) / vw.total_units) * vw.vests_per_unit) as member_vests,
           NOW(),
           @batch_id
    FROM sbi.member m
    JOIN sbi.vw_metrics vw
    WHERE m.balance_rshares > LEAST((m.shares + m.bonus_shares), 10) * vw.max_vote;


    -- Upsert into tokenholders
    INSERT INTO sbi.tokenholders (member_name, abc_vests)
    SELECT abc.account,
           abc.member_vests
    FROM sbi.balance_conversion as abc    
    WHERE abc.batch_id = @batch_id
    ON DUPLICATE KEY UPDATE
       abc_vests = abc_vests + VALUES(abc_vests);
    

	-- Update balances down to cutoff for this batch
	UPDATE sbi.member AS m
	JOIN sbi.balance_conversion AS abc
	  ON abc.account = m.account
	SET m.balance_rshares = m.balance_rshares - abc.trimmed_rshares
	WHERE abc.batch_id = @batch_id;

	-- Trim inactive members to one year of accrual
	INSERT INTO audit_rshares_trim (
    	account_name,
    	old_rshares,
    	new_rshares,
   		reason,
    	batch_id
	)
	SELECT m.account,
       m.balance_rshares,
       (shares + bonus_shares) * 3650 * (
    	SELECT rshares_per_cycle 
    	FROM sbi.configuration
    	LIMIT 1
		),
       'inactive_account_trim',
       @batch_id
	FROM member AS m
	WHERE m.last_post   < DATE_SUB(NOW(), INTERVAL 365 DAY)
  		AND m.last_comment < DATE_SUB(NOW(), INTERVAL 91 DAY)
  		AND (m.shares + m.bonus_shares) > 0
  		AND m.balance_rshares > (m.shares + m.bonus_shares) * 3650 * (
        	SELECT rshares_per_cycle 
        	FROM sbi.configuration
        	LIMIT 1
  		);

	
	UPDATE `member`
	SET balance_rshares = (shares + bonus_shares) * 3650 * (
    	SELECT rshares_per_cycle 
    	FROM sbi.configuration
    	LIMIT 1
		)
	WHERE last_post   < DATE_SUB(NOW(), INTERVAL 365 DAY)
  		AND last_comment < DATE_SUB(NOW(), INTERVAL 91 DAY)
  		AND (shares + bonus_shares) > 0
  		AND balance_rshares > (shares + bonus_shares) * 3650 * (
    		SELECT rshares_per_cycle 
    		FROM sbi.configuration
    		LIMIT 1
		);
	
	-- Trim SBI accounts to nine max votes
	
		UPDATE member AS m
		JOIN sbi.vw_metrics AS vw
		SET m.balance_rshares = vw.max_vote * 9
		WHERE m.note = 'sbi'
	  		AND m.balance_rshares > vw.max_vote * 9;

    
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Final view structure for view `vw_member_status`
--

/*!50001 DROP VIEW IF EXISTS `vw_member_status`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`sbi`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_member_status` AS select `m`.`account` AS `account`,`m`.`balance_rshares` AS `balance_rshares`,`sr`.`skiplist` AS `skiplist`,case when coalesce(`sr`.`skiplist`,0) = 0 and `m`.`balance_rshares` > `conf`.`minimum_vote_threshold` * 3 then 1 else 0 end AS `vote_ready` from ((`sbi`.`member` `m` join `sbi_reporting`.`members` `sr` on(`sr`.`account` = `m`.`account`)) join `sbi`.`configuration` `conf` on(`conf`.`id` = 1)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vw_metrics`
--

/*!50001 DROP VIEW IF EXISTS `vw_metrics`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`sbi`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_metrics` AS select `totals`.`total_units` AS `total_units`,`toks`.`tokens` AS `tokens`,`accs`.`total_mana` AS `total_mana`,`accs`.`reward_vests` AS `reward_vests`,`accs`.`total_mana` / nullif(`totals`.`total_units` + `toks`.`tokens`,0) AS `mana_per_unit`,`accs`.`total_mana` / nullif(`totals`.`total_units` + `toks`.`tokens`,0) / 1000000 AS `vests_per_unit`,`toks`.`tokens` / nullif(`totals`.`total_units`,0) AS `token_ratio`,`accs`.`reward_vests` * (`toks`.`tokens` / nullif(`totals`.`total_units`,0)) AS `divs`,`accs`.`reward_vests` * least(`toks`.`tokens` / nullif(`totals`.`total_units`,0),1) / nullif(`toks`.`tokens`,0) AS `divs_per`,`conf`.`rshares_per_cycle` AS `accrual`,`conf`.`minimum_vote_threshold` AS `min_vote`,`accs`.`total_mana` * 0.02 AS `max_vote`,`accs`.`total_mana` * 0.02 / nullif(`conf`.`minimum_vote_threshold`,0) AS `max_multiplier`,`accs`.`total_mana` * 0.02 / nullif(`conf`.`minimum_vote_threshold`,0) * 0.021 AS `max_vote_value`,`conf`.`rshares_per_cycle` * 3650 / (`accs`.`total_mana` / nullif(`totals`.`total_units` + `toks`.`tokens`,0)) AS `implied_apr` from ((((select coalesce(sum(`m`.`shares` + `m`.`bonus_shares`),0) AS `total_units` from `member` `m` where `m`.`note` is null or `m`.`note` <> 'sbi') `totals` join (select coalesce(sum(`a`.`max_mana`),0) AS `total_mana`,coalesce(sum(cast(regexp_substr(`a`.`reward_vests`,'^[0-9]+(\\.[0-9]+)?') as decimal(20,8))),sum(cast(substring_index(`a`.`reward_vests`,' ',1) as decimal(20,8)))) AS `reward_vests` from `accounts` `a`) `accs`) join (select coalesce(sum(`t`.`tokens`),0) AS `tokens` from `tokenholders` `t` where `t`.`member_name` <> 'sbi-tokens') `toks`) join (select `configuration`.`rshares_per_cycle` AS `rshares_per_cycle`,`configuration`.`minimum_vote_threshold` AS `minimum_vote_threshold` from `configuration` where `configuration`.`id` = 1) `conf`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-04-30 18:23:24
