/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19  Distrib 10.11.13-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: sbi_steem_ops
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

USE `sbi_steem_ops`;

--
-- Table structure for table `curation_optimization`
--

DROP TABLE IF EXISTS `curation_optimization`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `curation_optimization` (
  `authorperm` varchar(300) DEFAULT NULL,
  `member` varchar(16) NOT NULL,
  `created` datetime NOT NULL,
  `best_time_delay` float NOT NULL,
  `best_curation_performance` float NOT NULL,
  `vote_rshares` bigint(20) NOT NULL,
  `updated` datetime NOT NULL,
  `vote_delay` float NOT NULL,
  `performance` float NOT NULL,
  PRIMARY KEY (`member`,`created`),
  KEY `ix_curation_optimization_1f3f6589887b9c0e` (`member`,`created`),
  KEY `idx_curation_authorperm` (`authorperm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `member_hist`
--

DROP TABLE IF EXISTS `member_hist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `member_hist` (
  `block_num` int(11) NOT NULL,
  `block_id` varchar(40) NOT NULL,
  `trx_id` varchar(40) NOT NULL,
  `trx_num` int(11) NOT NULL,
  `op_num` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(30) NOT NULL,
  `author` varchar(16) DEFAULT NULL,
  `permlink` varchar(256) DEFAULT NULL,
  `parent_author` varchar(16) DEFAULT NULL,
  `parent_permlink` varchar(256) DEFAULT NULL,
  `voter` varchar(16) DEFAULT NULL,
  `weight` int(11) DEFAULT NULL,
  PRIMARY KEY (`block_num`,`trx_id`,`op_num`),
  KEY `author` (`author`),
  KEY `voter` (`voter`),
  KEY `type` (`type`),
  KEY `block_num` (`block_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `posts_comments`
--

DROP TABLE IF EXISTS `posts_comments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `posts_comments` (
  `authorperm` varchar(300) NOT NULL,
  `author` varchar(16) NOT NULL,
  `created` datetime NOT NULL,
  `block` int(11) DEFAULT NULL,
  `voted` tinyint(1) NOT NULL DEFAULT 0,
  `rshares` bigint(20) NOT NULL DEFAULT 0,
  `main_post` tinyint(1) NOT NULL DEFAULT 0,
  `skip` tinyint(1) NOT NULL DEFAULT 0,
  `comment_to_old` tinyint(1) NOT NULL DEFAULT 0,
  `vote_delay` float NOT NULL DEFAULT 900,
  `voted_after` float DEFAULT NULL,
  PRIMARY KEY (`author`,`created`),
  KEY `created` (`created`),
  KEY `author` (`author`),
  KEY `ix_posts_comments_83abfc77eaacd310` (`author`,`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi10_ops`
--

DROP TABLE IF EXISTS `sbi10_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi10_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi2_ops`
--

DROP TABLE IF EXISTS `sbi2_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi2_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`),
  KEY `op_acc_index_2` (`op_acc_index`,`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi3_ops`
--

DROP TABLE IF EXISTS `sbi3_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi3_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi4_ops`
--

DROP TABLE IF EXISTS `sbi4_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi4_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi5_ops`
--

DROP TABLE IF EXISTS `sbi5_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi5_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi6_ops`
--

DROP TABLE IF EXISTS `sbi6_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi6_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi7_ops`
--

DROP TABLE IF EXISTS `sbi7_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi7_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi8_ops`
--

DROP TABLE IF EXISTS `sbi8_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi8_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi9_ops`
--

DROP TABLE IF EXISTS `sbi9_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi9_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`op_acc_index`),
  KEY `block` (`block`),
  KEY `op_acc_index` (`op_acc_index`),
  KEY `ix_sbi9_ops_c5435d716e60d478` (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sbi_ops`
--

DROP TABLE IF EXISTS `sbi_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `sbi_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` bigint(19) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` bigint(19) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  KEY `op_acc_index` (`op_acc_index`),
  KEY `block` (`block`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `steembasicincome_ops`
--

DROP TABLE IF EXISTS `steembasicincome_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `steembasicincome_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL,
  PRIMARY KEY (`op_acc_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transfers`
--

DROP TABLE IF EXISTS `transfers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `transfers` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `from` varchar(50) NOT NULL,
  `to` varchar(50) NOT NULL,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `memo` varchar(2048) DEFAULT NULL,
  `op_type` varchar(50) NOT NULL,
  PRIMARY KEY (`op_acc_index`,`op_acc_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `vote_ops`
--

DROP TABLE IF EXISTS `vote_ops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `vote_ops` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `source_table` enum('sbi_ops','sbi2_ops','sbi3_ops','sbi4_ops','sbi5_ops','sbi6_ups','sbi7_ops','sbi8_ops','sbi9_ops','sbi10_ops') NOT NULL,
  `virtual_op` bigint(20) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(32) NOT NULL,
  `voter` varchar(64) DEFAULT NULL,
  `author` varchar(64) DEFAULT NULL,
  `permlink` varchar(255) DEFAULT NULL,
  `rshares` varchar(255) DEFAULT NULL,
  `weight` varchar(32) DEFAULT NULL,
  `op_dict` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`op_dict`)),
  PRIMARY KEY (`id`),
  KEY `idx_type` (`type`),
  KEY `idx_timestamp` (`timestamp`),
  KEY `idx_rshares` (`rshares`),
  KEY `idx_source` (`source_table`)
) ENGINE=InnoDB AUTO_INCREMENT=1966051 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'sbi_steem_ops'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-04-30 18:23:24
