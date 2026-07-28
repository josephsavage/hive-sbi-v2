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
-- PROD STATE MEASURED 2026-07-28 (re-run the pre-checks; do not trust this blind)
--   step 1 virtual_tokens column ................. ALREADY APPLIED — skip
--   step 2 tokens generated expression ........... ALREADY APPLIED — skip
--   step 3 status enum includes 'PENDING' ........ ALREADY APPLIED — skip
--   step 4 source_trx_id column + index .......... NOT APPLIED — run this
-- i.e. prod is post-#138 / pre-#139. Step 4 is the only DDL required, and it must
-- land BEFORE the code deploy (insert_pending_issuance writes source_trx_id on
-- every issuance) and BEFORE the LEGACY CLEANUP backfill at the bottom.
-- -----------------------------------------------------------------------------

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
--
-- STOP THE PIPELINE FIRST:
--     systemctl stop sbirunner
-- sbirunner.service is Restart=always and loops continuously, and
-- hsbi_token_snapshot writes token_issuance_log every cycle. DDL on that table
-- takes a metadata lock the running job will block on, and the LEGACY CLEANUP
-- below (UPDATE trx_id / DELETE) races a live reconciliation pass that is reading
-- and updating the same rows. Restart only after the post-checks pass.
--
-- Note on ADD COLUMN: MariaDB applies it instantly only when the new column lands
-- at the end of the row. Confirm `rationale` is currently the last column
-- (ORDINAL_POSITION) — if it is not, `AFTER rationale` rebuilds the whole table.
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
-- these as duplicate SUCCESS rows for chain issues it could not tie back to a log
-- row; the current code updates the original row in place instead. They do NOT
-- affect the Management cap (which sums rationale='Management' only), but any
-- report summing SUCCESS units counts the same issuance twice.
--
-- WHY THESE ARE ALL 'Unit Conversion': that path logged the originating *transfer*
-- hash in trx_id, never the Hive Engine issue hash, so its chain op never matched
-- the trx_id check and fell through to the orphan branch on every pass. pik /
-- Pending Balance Conversion / Management all log the real issue trx_id and were
-- never affected.
--
-- NOTE ON TIMESTAMPS: PR #138's INSERT omitted issued_at, so it defaulted to
-- current_timestamp() — the RECONCILIATION-PASS time, not the issuance time. Do
-- not pair on a narrow window around it, and do not treat r.issued_at as when the
-- tokens were minted. The original always precedes the duplicate.

-- 1. Measure the overstatement (record this before changing anything).
SELECT rationale, status, COUNT(*) AS row_count, SUM(units) AS units,
       MIN(issued_at) AS first_seen, MAX(issued_at) AS last_seen
FROM token_issuance_log
GROUP BY rationale, status
ORDER BY units DESC;

-- 2. Backfill source_trx_id for legacy Unit Conversion rows. MUST run BEFORE
--    step 4: those rows hold the originating transfer hash in trx_id, and step 4
--    overwrites it with the issue hash. This also makes
--    hivesbi.issuance_log.has_issuance_for_source effective for historic
--    transfers, which otherwise match nothing (source_trx_id IS NULL).
UPDATE token_issuance_log
SET source_trx_id = trx_id
WHERE rationale = 'Unit Conversion'
  AND source_trx_id IS NULL;

-- 3. Pair each 'reconciled' row with its original (same recipient + units, an
--    earlier SUCCESS row under a real rationale). Review the output before acting.
SELECT
    r.id AS dup_id,
    r.trx_id AS chain_trx,
    r.recipient,
    r.units,
    r.issued_at AS reconciled_at,
    s.id AS original_id,
    s.rationale AS original_rationale,
    s.trx_id AS original_trx,
    s.issued_at AS original_issued_at,
    TIMESTAMPDIFF(MINUTE, s.issued_at, r.issued_at) AS gap_minutes
FROM token_issuance_log r
LEFT JOIN token_issuance_log s
       ON  s.id        <> r.id
       AND s.recipient  = r.recipient
       AND s.units      = r.units
       AND s.status     = 'SUCCESS'
       AND s.rationale <> 'reconciled'
       AND s.issued_at <= r.issued_at
       AND s.issued_at >= r.issued_at - INTERVAL 12 HOUR
WHERE r.rationale = 'reconciled'
ORDER BY r.issued_at, gap_minutes;

-- 3b. Ambiguous rows: more than one candidate original. Resolve these by hand
--     (take the nearest in time) before running the deletes.
SELECT r.id AS dup_id, r.recipient, r.units, r.issued_at,
       COUNT(s.id) AS candidate_originals
FROM token_issuance_log r
JOIN token_issuance_log s
       ON  s.id        <> r.id
       AND s.recipient  = r.recipient
       AND s.units      = r.units
       AND s.status     = 'SUCCESS'
       AND s.rationale <> 'reconciled'
       AND s.issued_at <= r.issued_at
       AND s.issued_at >= r.issued_at - INTERVAL 12 HOUR
WHERE r.rationale = 'reconciled'
GROUP BY r.id, r.recipient, r.units, r.issued_at
HAVING COUNT(s.id) > 1;

-- 4. For each confirmed pair: move the real chain trx_id onto the original row,
--    then delete the duplicate (run per id after reviewing step 3):
--      UPDATE token_issuance_log SET trx_id = '<chain_trx>' WHERE id = <original_id>;
--      DELETE FROM token_issuance_log WHERE id = <dup_id>;
--
-- A 'reconciled' row with no pair at 12h is NOT automatically a genuine
-- out-of-band mint — widen to 48h first. Only a row that stays unpaired after
-- that is worth investigating as a real unlogged issuance.

-- 5. Verify: expect zero 'reconciled' rows, and SUCCESS units down by exactly the
--    amount recorded in step 1.
SELECT rationale, status, COUNT(*) AS row_count, SUM(units) AS units
FROM token_issuance_log
GROUP BY rationale, status
ORDER BY units DESC;

-- -----------------------------------------------------------------------------
-- OTHER NON-CANONICAL RATIONALES — decide, do not delete blindly
-- -----------------------------------------------------------------------------
-- Two more values exist that are not in the valid set. Neither is a duplicate, so
-- neither is covered by the cleanup above:
--   rationale='FAILURE' — status FAILURE, a legacy path that wrote the status into
--     the rationale column. Never summed (status is FAILURE), but it shows up as a
--     bogus category in any report that GROUPs BY rationale. Safe to relabel.
--   rationale='Reissue' — status SUCCESS, a one-off manual event. This is REAL
--     issuance: relabel or document it, never delete it.
SELECT rationale, status, COUNT(*) AS row_count, SUM(units) AS units,
       MIN(issued_at) AS first_seen, MAX(issued_at) AS last_seen
FROM token_issuance_log
WHERE rationale NOT IN ('pik', 'Pending Balance Conversion', 'Unit Conversion',
                        'Management')
   OR rationale IS NULL
GROUP BY rationale, status;
