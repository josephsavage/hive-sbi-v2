CREATE DATABASE IF NOT EXISTS `sbi`;
CREATE DATABASE IF NOT EXISTS `sbi_steem_ops`;
CREATE DATABASE IF NOT EXISTS `sbi_reporting`;

CREATE USER IF NOT EXISTS 'sbi'@'%' IDENTIFIED BY 'sbi';
GRANT ALL PRIVILEGES ON `sbi`.* TO 'sbi'@'%';
GRANT ALL PRIVILEGES ON `sbi_steem_ops`.* TO 'sbi'@'%';
GRANT ALL PRIVILEGES ON `sbi_reporting`.* TO 'sbi'@'%';
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS `sbi_reporting`.`members` (
  `account` varchar(16) NOT NULL,
  `skiplist` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
