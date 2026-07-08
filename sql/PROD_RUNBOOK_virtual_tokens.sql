-- =============================================================================
-- PROD RUNBOOK — virtual_tokens + write-ahead issuance logging
-- =============================================================================
-- This repo has no migration framework. Apply prod changes BY HAND, one statement
-- at a time, after running the pre-checks and confirming current state. Do NOT
-- run sql/20260602_virtual_tokens.sql against prod — that procedure is for dev.
--
-- Target DB: sbi   (mysql on the prod VPS)
-- All statements are additive/non-destructive except the generated-column metadata
-- update:
--   1. ADD COLUMN virtual_tokens (defaults 0.000 for every existing row)
--   2. MODIFY the VIRTUAL generated `tokens` column to add virtual_tokens
--      (metadata-only; VIRTUAL columns store no data)
--   3. MODIFY token_issuance_log.status enum to add 'PENDING'
--   4. ADD COLUMN token_issuance_log.source_trx_id + index
-- =============================================================================

USE `sbi`;

-- -----------------------------------------------------------------------------
-- PRE-CHECKS — run these first and read the output before changing anything.
-- -----------------------------------------------------------------------------

-- 0. Time zone sanity. Issuance intent timestamps (token_issuance_log.issued_at)
--    are written by the app as UTC wall time and matched against chain (UTC)
--    timestamps within a ±minutes window; issued_at is a TIMESTAMP column, so a
--    non-UTC session/system time zone silently shifts every stored instant and
--    reconciliation windows never match (rows would eventually be failed and
--    re-issued while the tokens were actually minted).
--    Expect: SYSTEM / SYSTEM / UTC (or explicit '+00:00' / 'UTC').
SELECT @@global.time_zone, @@session.time_zone, @@system_time_zone;

-- Expect: virtual_tokens absent; tokens = `liquid_tokens` + `LP_tokens`
SELECT COLUMN_NAME, COLUMN_TYPE, GENERATION_EXPRESSION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'tokenholders'
  AND COLUMN_NAME IN ('liquid_tokens', 'LP_tokens', 'virtual_tokens', 'tokens');

-- Expect: status present; source_trx_id may be absent before step 4.
SELECT COLUMN_NAME, COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'token_issuance_log'
  AND COLUMN_NAME IN ('status', 'source_trx_id');

-- Expect: idx_source_trx_id absent before step 4.
SELECT INDEX_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'token_issuance_log'
  AND INDEX_NAME = 'idx_source_trx_id';

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

-- 4. Add origin/source transaction storage for token issuance rows.
ALTER TABLE `token_issuance_log`
    ADD COLUMN `source_trx_id` varchar(100) DEFAULT NULL AFTER `rationale`;

ALTER TABLE `token_issuance_log`
    ADD INDEX `idx_source_trx_id` (`source_trx_id`);

-- -----------------------------------------------------------------------------
-- POST-CHECKS — confirm the new state.
-- -----------------------------------------------------------------------------

-- Expect: virtual_tokens present; tokens generation expression includes virtual_tokens
SELECT COLUMN_NAME, COLUMN_TYPE, GENERATION_EXPRESSION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'tokenholders'
  AND COLUMN_NAME IN ('virtual_tokens', 'tokens');

-- Expect: status includes PENDING and source_trx_id is present.
SELECT COLUMN_NAME, COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'token_issuance_log'
  AND COLUMN_NAME IN ('status', 'source_trx_id');

-- Expect: idx_source_trx_id present.
SELECT INDEX_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = 'sbi'
  AND TABLE_NAME = 'token_issuance_log'
  AND INDEX_NAME = 'idx_source_trx_id';

-- Sanity: tokens should equal liquid_tokens + LP_tokens for every row right after
-- step 1/2 (virtual_tokens defaults to 0 until hsbi_check_delegation populates it).
SELECT COUNT(*) AS rows_where_tokens_mismatch
FROM `tokenholders`
WHERE `tokens` <> (`liquid_tokens` + `LP_tokens` + `virtual_tokens`);

-- After deploying the code, run hsbi_check_delegation.py once to refresh the
-- derived Management virtual token allocation. Verify josephsavage equals 10% of
-- all other virtual_tokens (plus 2x his own delegated HP, if he delegates).
SELECT
    mgmt.member_name,
    mgmt.virtual_tokens AS josephsavage_virtual_tokens,
    ROUND(others.non_management_virtual_tokens * 0.10, 3) AS expected_virtual_tokens
FROM `tokenholders` AS mgmt
CROSS JOIN (
    SELECT COALESCE(SUM(virtual_tokens), 0) AS non_management_virtual_tokens
    FROM `tokenholders`
    WHERE member_name <> 'josephsavage'
) AS others
WHERE mgmt.member_name = 'josephsavage';

-- -----------------------------------------------------------------------------
-- LEGACY CLEANUP — rationale='reconciled' rows (PR #138 artifact)
-- -----------------------------------------------------------------------------
-- 'reconciled' was never a valid rationale (valid: pik, Pending Balance
-- Conversion, Unit Conversion, Management). PR #138's reconciliation inserted
-- these as duplicate rows for issuances whose real log row had trx_id='N/A';
-- the current code updates the original row in place instead. Existing rows do
-- not affect the Management cap (which sums rationale='Management' only), but
-- they pollute the audit trail.
--
-- Review first: pair each 'reconciled' row with its likely original (same
-- recipient + units, SUCCESS, uncaptured trx_id, within the match window).
SELECT
    r.id  AS reconciled_id,
    r.trx_id,
    r.recipient,
    r.units,
    r.issued_at,
    s.id  AS original_id,
    s.rationale AS original_rationale,
    s.issued_at AS original_issued_at
FROM token_issuance_log r
LEFT JOIN token_issuance_log s
       ON s.recipient = r.recipient
      AND s.units = r.units
      AND s.status = 'SUCCESS'
      AND s.trx_id = 'N/A'
      AND s.issued_at BETWEEN r.issued_at - INTERVAL 35 MINUTE
                          AND r.issued_at + INTERVAL 35 MINUTE
WHERE r.rationale = 'reconciled'
ORDER BY r.issued_at;

-- For each confidently paired row: move the chain trx_id onto the original row,
-- then delete the duplicate (run per id after reviewing the SELECT above):
--   UPDATE token_issuance_log SET trx_id = '<r.trx_id>' WHERE id = <original_id>;
--   DELETE FROM token_issuance_log WHERE id = <reconciled_id>;
--
-- A 'reconciled' row with NO pair is the only record of a genuine out-of-band
-- mint — investigate it before deleting anything.
