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
END;;

CALL migrate_virtual_tokens();;

DROP PROCEDURE IF EXISTS migrate_virtual_tokens;;

DELIMITER ;
