USE `sbi`;

DELIMITER ;;

DROP PROCEDURE IF EXISTS migrate_virtual_tokens;;

CREATE PROCEDURE migrate_virtual_tokens()
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'tokenholders'
          AND COLUMN_NAME = 'virtual_tokens'
    ) THEN
        ALTER TABLE tokenholders
            ADD COLUMN virtual_tokens decimal(15,3) NOT NULL DEFAULT 0.000
            AFTER LP_tokens;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'tokenholders'
          AND COLUMN_NAME = 'tokens'
          AND GENERATION_EXPRESSION LIKE '%virtual_tokens%'
    ) THEN
        ALTER TABLE tokenholders
            MODIFY COLUMN tokens decimal(15,3)
            GENERATED ALWAYS AS (`liquid_tokens` + `LP_tokens` + `virtual_tokens`)
            VIRTUAL;
    END IF;

    -- Add the PENDING status used by write-ahead token issuance logging.
    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'token_issuance_log'
          AND COLUMN_NAME = 'status'
          AND COLUMN_TYPE LIKE '%PENDING%'
    ) THEN
        ALTER TABLE token_issuance_log
            MODIFY COLUMN status enum('SUCCESS','FAILURE','PENDING') NOT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'token_issuance_log'
          AND COLUMN_NAME = 'source_trx_id'
    ) THEN
        ALTER TABLE token_issuance_log
            ADD COLUMN source_trx_id varchar(100) DEFAULT NULL
            AFTER rationale;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'token_issuance_log'
          AND INDEX_NAME = 'idx_source_trx_id'
    ) THEN
        ALTER TABLE token_issuance_log
            ADD INDEX idx_source_trx_id (source_trx_id);
    END IF;
END;;

CALL migrate_virtual_tokens();;

DROP PROCEDURE IF EXISTS migrate_virtual_tokens;;

DELIMITER ;
