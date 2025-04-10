-- ==================================================
-- File:        hpc_3_lakehouse_import_facts_v1.sql
-- Purpose:     Import cost breakdown facts into lakehouse fact table
-- Author:      APMB/MATS - Sys&Dev Team
-- Environment: PostgreSQL
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

--------------

-- Extracts caseload data from the source system, maps it to lakehouse dimensions,
-- and inserts both totals and data matrix metrics as facts into lake.lakefact_x.

WITH raw_data AS (
    SELECT
        lp.id AS planId,
        lf.id AS fieldClusterId,
        luy.id AS yearId,
        lgc.id AS globalClusterId,
        ll.id AS locationId,
        av.value -> 'metrics' -> 'values' -> 'disaggregated' -> 'dataMatrix' AS data_matrix,
        av.value -> 'metrics' -> 'values' -> 'totals' AS totals_array
    FROM public.attachment a
    JOIN public."attachmentVersion" av 
        ON a.id = av."attachmentId" AND av."latestVersion" = TRUE
    JOIN public."governingEntity" ge 
        ON a."objectId" = ge.id AND ge."latestVersion" = TRUE
    JOIN public."governingEntityVersion" gev 
        ON gev."governingEntityId" = ge.id AND gev."latestVersion" = TRUE
    JOIN public."globalClusterAssociation" gca 
        ON gca."governingEntityId" = ge.id
    JOIN public.plan p 
        ON a."planId" = p.id
    JOIN public."planVersion" pv 
        ON pv."planId" = p.id AND pv."latestVersion" = TRUE
    JOIN public."planYear" py 
        ON p.id = py."planId"
    JOIN public."usageYear" uy 
        ON uy.id = py."usageYearId"
    LEFT JOIN public.location l 
        ON l.id = pv."focusLocationId"
    LEFT JOIN lake.lakedim_location ll 
        ON ll.sourceId = l."externalId" AND ll.source = 'HPC'
    JOIN lake.lakedim_plan lp 
        ON lp.sourceId = p.id::TEXT AND lp.source = 'HPC'
    JOIN lake.lakedim_fieldcluster lf 
        ON lf.sourceId = ge.id::TEXT AND lf.source = 'HPC'
    JOIN lake.lakedim_year luy 
        ON luy.sourceId = uy.id::TEXT AND luy.source = 'HPC'
    JOIN lake.lakedim_globalcluster lgc 
        ON lgc.sourceId = gca."globalClusterId"::TEXT AND lgc.source = 'HPC'
    WHERE a.type ILIKE 'caseLoad'
      AND a."objectType" ILIKE 'governingEntity'
),
totals_extracted_raw AS (
    SELECT 
        rd.planId,
        rd.fieldClusterId,
        rd.globalClusterId,
        rd.locationId,
        rd.yearId,
        TRIM(LOWER(total ->> 'type')) AS metric_name,
        (total ->> 'value') AS raw_value
    FROM raw_data rd,
         jsonb_array_elements(rd.totals_array) AS total
),
data_matrix_extracted_raw AS (
    SELECT 
        rd.planId,
        rd.fieldClusterId,
        rd.globalClusterId,
        rd.locationId,
        rd.yearId,
        TRIM(LOWER(dm ->> 'metricType')) AS metric_name,
        (dm ->> 'value') AS raw_value
    FROM raw_data rd,
         jsonb_array_elements(rd.data_matrix) AS dm
),
metric_lookup AS (
    SELECT id AS metricTypeId, TRIM(LOWER(name)) AS metric_name
    FROM lake.lakedim_metrictype
    WHERE source = 'HPC'
),
totals_cleaned AS (
    SELECT
        t.planId,
        t.fieldClusterId,
        t.globalClusterId,
		t.locationId,
        t.yearId,
        m.metricTypeId,
        'HPC' AS sourceType,
        t.raw_value::NUMERIC AS valueNum
    FROM totals_extracted_raw t
    LEFT JOIN metric_lookup m ON t.metric_name = m.metric_name
    WHERE t.raw_value ~ '^-?[0-9]+(\.[0-9]+)?$'
),
data_matrix_cleaned AS (
    SELECT
        d.planId,
        d.fieldClusterId,
        d.globalClusterId,
        d.locationId,
        d.yearId,
        m.metricTypeId,
        'HPC' AS sourceType,
        d.raw_value::NUMERIC AS valueNum
    FROM data_matrix_extracted_raw d
    LEFT JOIN metric_lookup m ON d.metric_name = m.metric_name
    WHERE d.raw_value ~ '^-?[0-9]+(\.[0-9]+)?$'
)
-- Final insert into lake.lakefact_x
INSERT INTO lake.lakefact_x (
    planId,
    fieldClusterId,
    globalClusterId,
    yearId,
	locationId,
    metricTypeId,
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
	locationId,
    metricTypeId,
    'CaseLoad',
    sourceType,
    valueNum,
    NULL::VARCHAR,
    NULL::DATE,
    NULL::JSONB,
    NOW(),
    NULL::TIMESTAMP,
    'HPC',
    planId::TEXT,
    'approved',
    'SYSTEM',
    NOW()
FROM totals_cleaned
UNION ALL
SELECT 
    planId,
    fieldClusterId,
    globalClusterId,
    yearId,
	locationId,
    metricTypeId,
    'CaseLoad',
    sourceType,
    valueNum,
    NULL::VARCHAR,
    NULL::DATE,
    NULL::JSONB,
    NOW(),
    NULL::TIMESTAMP,
    'HPC',
    planId::TEXT,
    'approved',
    'SYSTEM',
    NOW()
FROM data_matrix_cleaned;