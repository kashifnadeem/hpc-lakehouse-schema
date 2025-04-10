-- ==================================================
-- File:        hpc_3_lakehouse_import_facts_v1.sql
-- Purpose:     Import cost breakdown facts into lakehouse fact table
-- Author:      APMB/MATS - Sys&Dev Team
-- Environment: PostgreSQL 14+
-- ==================================================

-- ***************************************************************
-- Script Description:
-- This ETL script extracts cost, caseLoad and other data from HPC attachments 
-- and loads them into the `lake.lakefact_x` table. It maps values 
-- from JSON structures to dimension keys using the lakehouse dimension tables. 
-- Each record is categorized by fact type "Cost" and source type "HPC".
--
-- This script is designed for full refresh only — no incremental logic 
-- is currently implemented. Therefore, **you must truncate `lake.lakefact_x`**
-- or target only specific fact types manually before rerunning it.
--
-- The `originalJson` is selected for audit purposes during development
-- but is not inserted into the final table.
--
-- ***************************************************************
-- How to run:
-- 1. Open your SQL client and connect to the target lakehouse database.
-- 2. Ensure the lakehouse schema and all dimension tables are up-to-date.
-- 3. **Truncate or clean `lake.lakefact_x` before execution**, as this is not incremental.
--    Example: `TRUNCATE lake.lakefact_x WHERE factType = 'Cost';`
-- 4. Copy and execute this full script in a query window.
-- 5. Validate the inserted records by checking lake.lakefact_x.
-- ***************************************************************
  
  
-- This query extracts cost-related breakdowns from HPC attachments 
-- and inserts them into the lakehouse fact table `lake.lakefact_x`. It maps source data to dimension IDs and
-- captures relevant metadata (like fact type, timestamps, status).

WITH raw_data AS (
    SELECT
        lp.id AS planId,
        lf.id AS fieldClusterId,
        a.id AS attachmentId,
        av.id AS attachmentVersionId,
        luy.id AS yearId,
        av.value AS originalJson, -- not saving in the db but only for audit,
        av.value -> 'metrics' -> 'values' -> 'disaggregated' -> 'dataMatrix' AS data_matrix, -- Data matrix from JSON
        av.value -> 'breakdownByGlobalCluster' AS breakdownArray
    FROM public.attachment a
    JOIN public."attachmentVersion" av ON a.id = av."attachmentId" AND av."latestVersion" = TRUE
    JOIN public."governingEntity" ge ON a."objectId" = ge.id AND ge."latestVersion" = TRUE
    JOIN public."governingEntityVersion" gev ON gev."governingEntityId" = ge.id AND gev."latestVersion" = TRUE
    JOIN public.plan p ON a."planId" = p.id
    JOIN public."planYear" py ON p.id = py."planId"
    JOIN public."usageYear" uy ON uy.id = py."usageYearId"
    JOIN lake.lakedim_plan lp ON lp.sourceId::VARCHAR = p.id::VARCHAR
    JOIN lake.lakedim_fieldcluster lf ON lf.sourceId::VARCHAR = ge.id::VARCHAR
    JOIN lake.lakedim_year luy ON luy.sourceId::VARCHAR = uy.id::VARCHAR
    WHERE a.type ILIKE 'cost'
      AND a."objectType" ILIKE 'governingEntity'
     AND (p.id::VARCHAR, 'HPC') IN (
    SELECT sourceId, source FROM lake.lakedim_plan)
),
breakdown_data AS (
    SELECT 
        rd.planId,
        rd.fieldClusterId,
        rd.yearId,
        (item ->> 'objectId')::INT AS globalClusterId,
        (item ->> 'cost')::NUMERIC AS valueNum,
        rd.originalJson
    FROM raw_data rd,
    jsonb_array_elements(rd.breakdownArray) AS item
),
final_data AS (
    SELECT
        planId,
        fieldClusterId,
        globalClusterId,
        yearId,
        'Cost' AS factType,
        'HPC' AS sourceType,
        valueNum,
        NULL::VARCHAR AS valueStr,
        NULL::DATE AS valueDate,
        NULL::JSONB AS valueObject,
        NOW() AS effectiveFrom,
        NULL::TIMESTAMP AS effectiveTo,
        'HPC' AS source,
        planId::VARCHAR AS sourceId,
        'approved' AS dataStatus,
        'SYSTEM' AS createdBy,
        NOW() AS createdAt,
        originalJson
    FROM breakdown_data
    WHERE valueNum IS NOT NULL
)
-- Insert into fact table (excludes originalJson)
INSERT INTO lake.lakefact_x (
    planId,
    fieldClusterId,
    globalClusterId,
    yearId,
    factType,
    sourceType,
    valueNum,
    valueStr,
    valueDate,
    valueObject,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdBy,
    createdAt
)
SELECT
    planId,
    fieldClusterId,
    globalClusterId,
    yearId,
    factType,
    sourceType,
    valueNum,
    valueStr,
    valueDate,
    valueObject,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdBy,
    createdAt
FROM final_data;