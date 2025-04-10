-------------------------------------------
  
  -- This query extracts cost-related breakdowns from HPC attachments (specifically `breakdownByGlobalCluster`)
-- and inserts them into the lakehouse fact table `lake.lakefact_x`. It maps source data to dimension IDs and
-- captures relevant metadata (like fact type, timestamps, status) while keeping the original JSON for audit.


WITH raw_data AS (
    SELECT
        lp.id AS planId,
        lf.id AS fieldClusterId,
        a.id AS attachmentId,
        av.id AS attachmentVersionId,
        luy.id AS usageYearId,
        av.value AS originalJson,
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
    JOIN lake.lakedim_usageyear luy ON luy.sourceId::VARCHAR = uy.id::VARCHAR
    WHERE a.type ILIKE 'cost'
      AND a."objectType" ILIKE 'governingEntity'
     AND (p.id::VARCHAR, 'HPC') IN (
    SELECT sourceId, source FROM lake.lakedim_plan)
),
breakdown_data AS (
    SELECT 
        rd.planId,
        rd.fieldClusterId,
        rd.usageYearId,
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
        usageYearId,
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
    usageYearId,
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
    usageYearId,
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

----------------------

-- Extracts caseload data from the source system, maps it to lakehouse dimensions,
-- and inserts both totals and data matrix metrics as facts into lake.lakefact_x.

WITH raw_data AS (
    SELECT
        lp.id AS planId,
        lf.id AS fieldClusterId,
        luy.id AS yearId,
        lgc.id AS globalClusterId,
        av.value -> 'metrics' -> 'values' -> 'disaggregated' -> 'dataMatrix' AS data_matrix,
        av.value -> 'metrics' -> 'values' -> 'totals' AS totals_array
    FROM public.attachment a
    JOIN public."attachmentVersion" av ON a.id = av."attachmentId" AND av."latestVersion" = TRUE
    JOIN public."governingEntity" ge ON a."objectId" = ge.id AND ge."latestVersion" = TRUE
    JOIN public."governingEntityVersion" gev ON gev."governingEntityId" = ge.id AND gev."latestVersion" = TRUE
    JOIN public."globalClusterAssociation" gca ON gca."governingEntityId" = ge.id
    JOIN public.plan p ON a."planId" = p.id
    JOIN public."planYear" py ON p.id = py."planId"
    JOIN public."usageYear" uy ON uy.id = py."usageYearId"
    JOIN lake.lakedim_plan lp ON lp.sourceId = p.id::TEXT AND lp.source = 'HPC'
    JOIN lake.lakedim_fieldcluster lf ON lf.sourceId = ge.id::TEXT AND lf.source = 'HPC'
    JOIN lake.lakedim_year luy ON luy.sourceId = uy.id::TEXT AND luy.source = 'HPC'
    JOIN lake.lakedim_globalcluster lgc ON lgc.sourceId = gca."globalClusterId"::TEXT AND lgc.source = 'HPC'
    WHERE a.type ILIKE 'caseLoad'
      AND a."objectType" ILIKE 'governingEntity'
),
totals_extracted_raw AS (
    SELECT 
        rd.planId,
        rd.fieldClusterId,
        rd.globalClusterId,
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


=========================


------
WITH raw_data AS (
    -- Step 1: Extracting raw data from related tables
    SELECT
        lp.id AS planid,
        lp."version" AS planversion,
        lgc.id AS globalclusterid,
        lgc."version" AS globalclusterversion,
        lf.id AS fieldclusterid,
        lf."version" AS fieldclusterversion,
        a.id AS attachment_id,
        a."type" AS recordtype,
        a."objectType" AS objecttype,
        av.id AS attachmentVersion_id,
        luy.id AS usageyearid,
        luy."version" AS usageyearversion,
        av.value AS original_json,
        av.value -> 'metrics' -> 'values' -> 'disaggregated' -> 'dataMatrix' AS data_matrix,
        av.value -> 'metrics' -> 'values' -> 'totals' AS totals_array
    FROM public.attachment a    
    JOIN public."attachmentVersion" av 
        ON a.id = av."attachmentId" 
        AND av."latestVersion" = true 
        AND av."currentVersion" = true    
    JOIN public."governingEntity" ge 
        ON a."objectId" = ge.id 
        AND ge."currentVersion" = true 
        AND ge."latestVersion" = true
    JOIN public."governingEntityVersion" gev 
        ON gev."governingEntityId" = ge.id 
        AND gev."currentVersion" = true 
        AND gev."latestVersion" = true
    JOIN public."globalClusterAssociation" gca 
        ON gca."governingEntityId" = ge.id    
    JOIN public.plan p 
        ON a."planId" = p.id
    JOIN public."planYear" py 
        ON p.id = py."planId"
    JOIN public."usageYear" uy 
        ON uy.id = py."usageYearId"
    -- Join lakedim tables
    JOIN lake.lakedim_plan lp 
        ON lp.sourceid::VARCHAR = p.id::VARCHAR
    JOIN lake.lakedim_fieldcluster lf 
        ON lf.sourceid::VARCHAR = ge.id::VARCHAR
    JOIN lake.lakedim_usageyear luy 
        ON luy.sourceid::VARCHAR = uy.id::VARCHAR
    JOIN lake.lakedim_globalcluster lgc 
        ON lgc.sourceid::VARCHAR = gca."globalClusterId"::VARCHAR
    WHERE a.type LIKE 'caseLoad'
    AND a."objectType" LIKE 'governingEntity'
    AND a."planId" IN (1188, 1190, 1193, 1195)
),
totals_extracted AS (
    -- Step 2: Extract total values
    SELECT 
        rd.planid,
        rd.planversion,
        rd.globalclusterid,
        rd.globalclusterversion,
        rd.fieldclusterid,
        rd.fieldclusterversion,
        rd.usageyearid,
        rd.usageyearversion,
        (total ->> 'type') AS facttype,
        'HPC' AS sourcetype,
        (total ->> 'value')::numeric AS valuenum
    FROM raw_data rd,
    jsonb_array_elements(rd.totals_array) AS total
),
data_matrix_expanded AS (
    -- Step 3: Expanding data matrix JSON into structured format
    SELECT 
        rd.planid, 
        rd.planversion,
        rd.globalclusterid,
        rd.globalclusterversion,
        rd.fieldclusterid,
        rd.fieldclusterversion,
        rd.usageyearid,
        rd.usageyearversion,
        jsonb_array_elements(rd.data_matrix) AS data_matrix
    FROM raw_data rd
),
data_matrix_cleaned AS (
    -- Step 4: Extract values from the data matrix
    SELECT
        dme.planid,
        dme.planversion,
        dme.globalclusterid,
        dme.globalclusterversion,
        dme.fieldclusterid,
        dme.fieldclusterversion,
        dme.usageyearid,
        dme.usageyearversion,
        (dme.data_matrix ->> 'metricType') AS facttype,
        'HPC' AS sourcetype,
        CASE 
            WHEN dme.data_matrix ->> 'value' ~ '^-?[0-9]+(\.[0-9]+)?$' 
                THEN (dme.data_matrix ->> 'value')::numeric  
            ELSE NULL  
        END AS valuenum
    FROM data_matrix_expanded dme
)
-- Step 5: Insert transformed data into lakefact_x
INSERT INTO lake.lakefact_x (
    "version",
    emergencyid,
    emergencyversion,
    planid,
    planversion,
    organizationid,
    organizationversion,
    locationid,
    locationversion,
    globalclusterid,
    globalclusterversion,
    fieldclusterid,
    fieldclusterversion,
    projectid,
    projectversion,
    usageyearid,
    usageyearversion,
    populationtypeid,
    populationtypeversion,
    genderid,
    genderversion,
    agegroupid,
    agegroupversion,
    demographicid,
    demographicversion,
    facttype,
    sourcetype,
    valuenum,
    valuestr,
    valuedate,
    valueobject,
    createduser,
    createdtime,
    deleted,
    "current"
)
-- Insert totals
SELECT 
    1 AS "version",
    NULL::integer AS emergencyid,
    NULL::integer AS emergencyversion,
    te.planid,
    te.planversion,
    NULL::integer AS organizationid,
    NULL::integer AS organizationversion,
    NULL::integer AS locationid,
    NULL::integer AS locationversion,
    te.globalclusterid,
    te.globalclusterversion,
    te.fieldclusterid,
    te.fieldclusterversion,
    NULL::integer AS projectid,
    NULL::integer AS projectversion,
    te.usageyearid,
    te.usageyearversion,
    NULL::integer AS populationtypeid,
    NULL::integer AS populationtypeversion,
    NULL::integer AS genderid,
    NULL::integer AS genderversion,
    NULL::integer AS agegroupid,
    NULL::integer AS agegroupversion,
    NULL::integer AS demographicid,
    NULL::integer AS demographicversion,
    te.facttype,
    te.sourcetype,
    te.valuenum,
    NULL::varchar AS valuestr,
    NULL::date AS valuedate,
    NULL::jsonb AS valueobject,
    'SYSTEM' AS createduser,
    NOW() AS createdtime,
    FALSE AS deleted,
    TRUE AS "current"
FROM totals_extracted te
WHERE te.valuenum IS NOT NULL
UNION ALL
-- Insert data matrix values
SELECT 
    1 AS "version",
    NULL::integer AS emergencyid,
    NULL::integer AS emergencyversion,
    dmc.planid,
    dmc.planversion,
    NULL::integer AS organizationid,
    NULL::integer AS organizationversion,
    NULL::integer AS locationid,
    NULL::integer AS locationversion,
    dmc.globalclusterid,
    dmc.globalclusterversion,
    dmc.fieldclusterid,
    dmc.fieldclusterversion,
    NULL::integer AS projectid,
    NULL::integer AS projectversion,
    dmc.usageyearid,
    dmc.usageyearversion,
    NULL::integer AS populationtypeid,
    NULL::integer AS populationtypeversion,
    NULL::integer AS genderid,
    NULL::integer AS genderversion,
    NULL::integer AS agegroupid,
    NULL::integer AS agegroupversion,
    NULL::integer AS demographicid,
    NULL::integer AS demographicversion,
    dmc.facttype,
    dmc.sourcetype,
    dmc.valuenum,
    NULL::varchar AS valuestr,
    NULL::date AS valuedate,
    NULL::jsonb AS valueobject,
    'SYSTEM' AS createduser,
    NOW() AS createdtime,
    FALSE AS deleted,
    TRUE AS "current"
FROM data_matrix_cleaned dmc
WHERE dmc.valuenum IS NOT NULL;

--=========================================

--===============================
-- ****************************
-- Exctract and insert data in lakedim_fact table
----
-- Plan Cluster Cost data
WITH raw_data AS (
    -- Step 1: Extract raw cost data related to governing entities
    SELECT
        lp.id AS plan_id, -- lakedim_plan ID
        lp."version" AS planversion, -- Plan version
        lf.id AS fieldclusterid, -- lakedim_fieldcluster ID
        lf."version" AS fieldclusterversion, -- Field cluster version
        a.id AS attachment_id, -- Attachment ID
        a."type" AS record_type, -- Record type, e.g., "cost"
        a."objectType" AS object_type, -- Object type, e.g., "governingEntity"
        av.id AS attachmentVersion_id, -- Attachment version ID
        luy.id AS usageyearid, -- lakedim_usageyear ID
        luy."version" AS usageyearversion, -- Usage year version
        av.value AS original_json, -- Original JSON data from the attachment
        av.value -> 'metrics' -> 'values' -> 'disaggregated' -> 'dataMatrix' AS data_matrix, -- Data matrix from JSON
        av.value -> 'breakdown' AS breakdown_array -- Breakdown array containing cost data
    FROM public.attachment a
    JOIN public."attachmentVersion" av 
        ON a.id = av."attachmentId" 
        AND av."latestVersion" = TRUE
    JOIN public."governingEntity" ge 
        ON a."objectId" = ge.id 
        AND ge."currentVersion" = TRUE 
        AND ge."latestVersion" = TRUE
    JOIN public."governingEntityVersion" gev 
        ON gev."governingEntityId" = ge.id 
        AND gev."currentVersion" = TRUE 
        AND gev."latestVersion" = TRUE
    JOIN public.plan p 
        ON a."planId" = p.id
    JOIN public."planYear" py 
        ON p.id = py."planId"
    JOIN public."usageYear" uy 
        ON uy.id = py."usageYearId"
    -- Join lakedim tables from lake schema
    JOIN lake.lakedim_plan lp 
        ON lp.sourceid::VARCHAR = p.id::VARCHAR
    JOIN lake.lakedim_fieldcluster lf 
        ON lf.sourceid::VARCHAR = ge.id::VARCHAR
    JOIN lake.lakedim_usageyear luy 
        ON luy.sourceid::VARCHAR = uy.id::VARCHAR
    WHERE a.type LIKE 'cost' -- Only include attachments of type "cost"
    AND a."objectType" LIKE 'governingEntity' 
    AND a."planId" IN (1188, 1190, 1193, 1195) 
),
breakdown_data AS (
    -- Step 2: Parse the "breakdown" array to extract costs and associated metadata
    SELECT 
        rd.plan_id, -- Plan ID
        rd.planversion, -- Plan version
        rd.attachment_id, -- Attachment ID
        rd.fieldclusterid, -- Field cluster ID
        rd.fieldclusterversion, -- Field cluster version
        (breakdown_item ->> 'objectId')::int AS globalclusterid, -- Extract "objectId" as global cluster ID
        (breakdown_item ->> 'cost')::numeric AS valuenum -- Extract "cost" as numeric value
    FROM raw_data rd,
    jsonb_array_elements(rd.breakdown_array) AS breakdown_item -- Expand the "breakdown" array into rows
),
fact_data AS (
    -- Step 3: Map extracted values to `lakedim_*` IDs and versions
    SELECT 
        1 AS "version", -- Initial version
        NULL::int AS emergencyid, -- Default NULL for emergency ID
        NULL::int AS emergencyversion, -- Default NULL for emergency version
        bd.plan_id, -- Plan ID
        bd.planversion, -- Plan version
        NULL::int AS organizationid, -- Default NULL for organization ID
        NULL::int AS organizationversion, -- Default NULL for organization version
        NULL::int AS locationid, -- Default NULL for location ID
        NULL::int AS locationversion, -- Default NULL for location version
        lgc.id AS globalclusterid, -- Global cluster ID
        lgc."version" AS globalclusterversion, -- Global cluster version
        bd.fieldclusterid, -- Field cluster ID
        bd.fieldclusterversion, -- Field cluster version
        NULL::int AS projectid, -- Default NULL for project ID
        NULL::int AS projectversion, -- Default NULL for project version
        rd.usageyearid, -- Usage year ID
        rd.usageyearversion, -- Usage year version
        NULL::int AS populationtypeid, -- Default NULL for population type ID
        NULL::int AS populationtypeversion, -- Default NULL for population type version
        NULL::int AS genderid, -- Default NULL for gender ID
        NULL::int AS genderversion, -- Default NULL for gender version
        NULL::int AS agegroupid, -- Default NULL for age group ID
        NULL::int AS agegroupversion, -- Default NULL for age group version
        NULL::int AS demographicid, -- Default NULL for demographic ID
        NULL::int AS demographicversion, -- Default NULL for demographic version
        'Cost' AS facttype, -- Fact type
        'HPC' AS sourcetype, -- Source type
        bd.valuenum, -- Cost value from the breakdown array
        NULL::varchar AS valuestr, -- Default NULL for string value
        NULL::date AS valuedate, -- Default NULL for date value
        NULL::jsonb AS valueobject, -- Default NULL for JSON object
        'SYSTEM' AS createduser, -- User who created it
        NOW() AS createdtime, -- Timestamp
        FALSE AS deleted, -- Default deleted flag
        TRUE AS "current" -- Mark as current version
    FROM breakdown_data bd
    JOIN raw_data rd 
        ON rd.plan_id = bd.plan_id 
        AND rd.attachment_id = bd.attachment_id -- Match on plan and attachment ID
    LEFT JOIN lake.lakedim_globalcluster lgc 
        ON lgc.sourceid::VARCHAR = bd.globalclusterid::VARCHAR -- Join with `lakedim_globalcluster`
    WHERE bd.valuenum IS NOT NULL -- Only include rows where cost ("valuenum") is not null
)
-- Step 4: Insert parsed and validated data into `lake.lakefact_x`
INSERT INTO lake.lakefact_x (
    "version",
    emergencyid,
    emergencyversion,
    planid,
    planversion,
    organizationid,
    organizationversion,
    locationid,
    locationversion,
    globalclusterid,
    globalclusterversion,
    fieldclusterid,
    fieldclusterversion,
    projectid,
    projectversion,
    usageyearid,
    usageyearversion,
    populationtypeid,
    populationtypeversion,
    genderid,
    genderversion,
    agegroupid,
    agegroupversion,
    demographicid,
    demographicversion,
    facttype,
    sourcetype,
    valuenum,
    valuestr,
    valuedate,
    valueobject,
    createduser,
    createdtime,
    deleted,
    "current"
)
SELECT * FROM fact_data;

===============

----
-- If we take all the existing plans from the table and increment version but the question is if we are ony saving basic informaotin in the lake then it is just a repition
-- the solution is to only increment the version when there is a real change and the RPM is not just creating a new vresion using and marking it latest on each save.

SELECT 
    ROW_NUMBER() OVER (PARTITION BY pv."planId" ORDER BY pv."createdAt") AS version, -- Generates incremental version numbers per planId
    'HPC' AS source,
    p.id AS sourceid,
    pv."name",
    pv."createdAt" AS createdtime,
    false,
    pv."latestVersion" AS "current"
FROM public.plan p
JOIN public."planVersion" pv ON p.id = pv."planId"
where p.id = 1188
ORDER BY p.id, version