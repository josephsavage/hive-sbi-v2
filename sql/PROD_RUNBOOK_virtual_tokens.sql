-- =============================================================================
-- PROD RUNBOOK — virtual_tokens + write-ahead issuance logging
-- =============================================================================
-- This repo has no migration framework. Apply prod changes BY HAND, one statement
-- at a time, after running the pre-checks and confirming current state. Do NOT
-- run sql/20260602_virtual_tokens.sql against prod — that procedure is for dev.
--
-- Target DB: sbi   (mysql on the prod VPS)
-- All three statements are additive and non-destructive (no data rewrite):
--   1. ADD COLUMN virtual_tokens (defaults 0.000 for every existing row)
--   2. MODIFY the VIRTUAL generated `tokens` column to add virtual_tokens
--      (metadata-only; VIRTUAL columns store no data)
--   3. MODIFY token_issuance_log.status enum to add 'PENDING'
-- =============================================================================

USE `sbi`;

-- -----------------------------------------------------------------------------
-- PRE-CHECKS — run these first and read the output before changing anything.
-- -----------------------------------------------------------------------------

-- Expect: virtual_tokens absent; tokens = `liquid_tokens` + `LP_tokens`
SELECT COLUMN_NAME, COLUMN_TYPE, GENERATION_EXPRESSION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'tokenholders'
  AND COLUMN_NAME IN ('liquid_tokens', 'LP_tokens', 'virtual_tokens', 'tokens');

-- Expect: enum('SUCCESS','FAILURE')
SELECT COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'token_issuance_log'
  AND COLUMN_NAME = 'status';

-- -----------------------------------------------------------------------------
-- APPLY — run only the statements whose pre-check showed the OLD state.
-- Each is safe to skip if the pre-check already shows the new state.
-- -----------------------------------------------------------------------------

-- 1. Add the virtual_tokens column (positioned after LP_tokens for readability).
ALTER TABLE `tokenholders`
    ADD COLUMN `virtual_tokens` decimal(15,3) NOT NULL DEFAULT 0.000 AFTER `LP_tokens`;

-- 2. Fold virtual_tokens into the generated tokens column.
ALTER TABLE `tokenholders`
    MODIFY COLUMN `tokens` decimal(15,3)
    GENERATED ALWAYS AS (`liquid_tokens` + `LP_tokens` + `virtual_tokens`) VIRTUAL;

-- 3. Add the PENDING status for write-ahead issuance logging.
ALTER TABLE `token_issuance_log`
    MODIFY COLUMN `status` enum('SUCCESS','FAILURE','PENDING') NOT NULL;

-- -----------------------------------------------------------------------------
-- POST-CHECKS — confirm the new state.
-- -----------------------------------------------------------------------------

-- Expect: virtual_tokens present; tokens generation expression includes virtual_tokens
SELECT COLUMN_NAME, COLUMN_TYPE, GENERATION_EXPRESSION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'tokenholders'
  AND COLUMN_NAME IN ('virtual_tokens', 'tokens');

-- Expect: enum('SUCCESS','FAILURE','PENDING')
SELECT COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'token_issuance_log'
  AND COLUMN_NAME = 'status';

-- Sanity: tokens should equal liquid_tokens + LP_tokens for every row right after
-- step 1/2 (virtual_tokens defaults to 0 until hsbi_check_delegation populates it).
SELECT COUNT(*) AS rows_where_tokens_mismatch
FROM `tokenholders`
WHERE `tokens` <> (`liquid_tokens` + `LP_tokens` + `virtual_tokens`);
