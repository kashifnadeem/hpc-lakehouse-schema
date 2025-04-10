-- Insert HPC Plans
INSERT INTO lake.lakedim_plan (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
SELECT 
    pv."name",
    pv."createdAt" AS effectiveFrom,
    NULL AS effectiveTo,
    'HPC' AS source,
    p.id AS sourceId,
    'approved' AS dataStatus,
    now() AS createdAt  -- timestamp of ingestion into lake
FROM public.plan p
JOIN public."planVersion" pv 
    ON p.id = pv."planId"
WHERE pv."latestVersion" = true
ORDER BY p.id;

-- Insert HPC organizations
INSERT INTO lake.lakedim_organization (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
SELECT
    o.name AS name,
    o."createdAt" AS effectiveFrom,
    NULL AS effectiveTo,
    'HPC' AS source,
    o.id::TEXT AS sourceId,
    'approved' AS dataStatus,
    now() AS createdAt
FROM public.organization o
WHERE o.active = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM lake.lakedim_organization lo 
      WHERE lo.sourceId = o.id::TEXT
);

-- insert agegroup
INSERT INTO lake.lakedim_agegroup (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
) VALUES
    ('Zero', now(), NULL, 'HPC', '1', 'approved', now()),
    ('Zero-One', now(), NULL, 'HPC', '2', 'approved', now()),
    ('Zero-Two', now(), NULL, 'HPC', '3', 'approved', now()),
    ('Zero-Three', now(), NULL, 'HPC', '4', 'approved', now()),
    ('Zero-Four', now(), NULL, 'HPC', '5', 'approved', now()),
    ('Zero-Five', now(), NULL, 'HPC', '6', 'approved', now()),
    ('One-Two', now(), NULL, 'HPC', '7', 'approved', now()),
    ('One-Three', now(), NULL, 'HPC', '8', 'approved', now()),
    ('One-Four', now(), NULL, 'HPC', '9', 'approved', now()),
    ('One-Five', now(), NULL, 'HPC', '10', 'approved', now()),
    ('One-Six', now(), NULL, 'HPC', '11', 'approved', now()),
    ('One-Seven', now(), NULL, 'HPC', '12', 'approved', now()),
    ('One-Eight', now(), NULL, 'HPC', '13', 'approved', now()),
    ('One-Nine', now(), NULL, 'HPC', '14', 'approved', now()),
    ('One-Ten', now(), NULL, 'HPC', '15', 'approved', now()),
    ('One-Eleven', now(), NULL, 'HPC', '16', 'approved', now()),
    ('One-Twelve', now(), NULL, 'HPC', '17', 'approved', now()),
    ('One-Thirteen', now(), NULL, 'HPC', '18', 'approved', now()),
    ('One-Fourteen', now(), NULL, 'HPC', '19', 'approved', now()),
    ('One-Fifteen', now(), NULL, 'HPC', '20', 'approved', now()),
    ('One-Sixteen', now(), NULL, 'HPC', '21', 'approved', now()),
    ('One-Seventeen', now(), NULL, 'HPC', '22', 'approved', now()),
    ('Middle Childhood', now(), NULL, 'HPC', '23', 'approved', now()),
    ('Five-Seven', now(), NULL, 'HPC', '24', 'approved', now()),
    ('Five-Eight', now(), NULL, 'HPC', '25', 'approved', now()),
    ('Five-Nine', now(), NULL, 'HPC', '26', 'approved', now()),
    ('Five-Ten', now(), NULL, 'HPC', '27', 'approved', now()),
    ('Five-Eleven', now(), NULL, 'HPC', '28', 'approved', now()),
    ('Five-Twelve', now(), NULL, 'HPC', '29', 'approved', now()),
    ('Five-Thirteen', now(), NULL, 'HPC', '30', 'approved', now()),
    ('Five-Fourteen', now(), NULL, 'HPC', '31', 'approved', now()),
    ('Five-Fifteen', now(), NULL, 'HPC', '32', 'approved', now()),
    ('Five-Sixteen', now(), NULL, 'HPC', '33', 'approved', now()),
    ('Five-Seventeen', now(), NULL, 'HPC', '34', 'approved', now()),
    ('Adolescence', now(), NULL, 'HPC', '35', 'approved', now()),
    ('Eleven-Seventeen', now(), NULL, 'HPC', '36', 'approved', now()),
    ('Twelve-Seventeen', now(), NULL, 'HPC', '37', 'approved', now()),
    ('Thirteen-Seventeen', now(), NULL, 'HPC', '38', 'approved', now()),
    ('Eighteen-Twenty-Five', now(), NULL, 'HPC', '39', 'approved', now()),
    ('Eighteen-Fifty-Nine', now(), NULL, 'HPC', '40', 'approved', now()),
    ('Sixty', now(), NULL, 'HPC', '41', 'approved', now()),
    ('Sixty-Plus', now(), NULL, 'HPC', '42', 'approved', now()),
    ('Sixty-Seventy', now(), NULL, 'HPC', '43', 'approved', now()),
    ('Sixty-Eighty', now(), NULL, 'HPC', '44', 'approved', now()),
    ('Seventy-Plus', now(), NULL, 'HPC', '45', 'approved', now()),
    ('Seventy-Eighty', now(), NULL, 'HPC', '46', 'approved', now()),
    ('Eighty-Plus', now(), NULL, 'HPC', '47', 'approved', now());

-- Insert demograpic
INSERT INTO lake.lakedim_demographic (
    name, effectiveFrom, effectiveTo, source, sourceId, dataStatus, createdAt
) VALUES
    ('Children', now(), NULL, 'HPC', '1', 'approved', now()),
    ('Adults', now(), NULL, 'HPC', '2', 'approved', now()),
    ('Elderly', now(), NULL, 'HPC', '3', 'approved', now()),
    ('Boys', now(), NULL, 'HPC', '4', 'approved', now()),
    ('Girls', now(), NULL, 'HPC', '5', 'approved', now()),
    ('Men', now(), NULL, 'HPC', '6', 'approved', now()),
    ('Women', now(), NULL, 'HPC', '7', 'approved', now()),
    ('Households', now(), NULL, 'HPC', '8', 'approved', now()),
    ('Families', now(), NULL, 'HPC', '9', 'approved', now());

-- insert gender
INSERT INTO lake.lakedim_gender (
    name, effectiveFrom, effectiveTo, source, sourceId, dataStatus, createdAt
) VALUES
    ('Male', now(), NULL, 'HPC', '1', 'approved', now()),
    ('Female', now(), NULL, 'HPC', '2', 'approved', now()),
    ('Non-binary', now(), NULL, 'HPC', '3', 'approved', now());

-- insert population types
INSERT INTO lake.lakedim_populationtype (
    name, effectiveFrom, effectiveTo, source, sourceId, dataStatus, createdAt
) VALUES
    ('Host Communities', now(), NULL, 'HPC', '1', 'approved', now()),
    ('IDPs', now(), NULL, 'HPC', '2', 'approved', now()),
    ('Refugees', now(), NULL, 'HPC', '3', 'approved', now()),
    ('Returnees', now(), NULL, 'HPC', '4', 'approved', now()),
    ('Migrants', now(), NULL, 'HPC', '5', 'approved', now()),
    ('Vulnerable Groups', now(), NULL, 'HPC', '6', 'approved', now()),
    ('Disabled', now(), NULL, 'HPC', '7', 'approved', now()),
    ('Displaced', now(), NULL, 'HPC', '8', 'approved', now()),
    ('Non-displaced', now(), NULL, 'HPC', '9', 'approved', now()),
    ('Resident', now(), NULL, 'HPC', '10', 'approved', now()),
    ('Affected', now(), NULL, 'HPC', '11', 'approved', now()),
    ('Targeted', now(), NULL, 'HPC', '12', 'approved', now()),
    ('Other', now(), NULL, 'HPC', '13', 'approved', now());

-- insert into filed cluster
WITH ranked_entities AS (
    SELECT
        gev."governingEntityId" AS sourceId,
        gev."name" AS name,
        ROW_NUMBER() OVER (PARTITION BY gev."governingEntityId" ORDER BY gev."updatedAt" DESC) AS row_num
    FROM public."governingEntityVersion" gev
    WHERE gev."latestVersion" = TRUE
      AND COALESCE(TRIM(gev."name"), '') <> ''
)
INSERT INTO lake.lakedim_fieldcluster (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
SELECT
    name,
    now() AS effectiveFrom,
    NULL AS effectiveTo,
    'HPC' AS source,
    sourceId::VARCHAR,
    'approved' AS dataStatus,
    now() AS createdAt
FROM ranked_entities
WHERE row_num = 1;

-- insert global cluster
INSERT INTO lake.lakedim_globalcluster (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
SELECT
    gc.name,
    now() AS effectiveFrom,
    NULL AS effectiveTo,
    'HPC' AS source,
    gc.id::VARCHAR AS sourceId,
    'approved' AS dataStatus,
    now() AS createdAt
FROM public."globalCluster" gc;

-- insert projects
WITH ranked_projects AS (
    SELECT
        pv."projectId"::VARCHAR AS sourceId,
        pv.name,
        pv."currentRequestedFunds",
        ROW_NUMBER() OVER (PARTITION BY pv."projectId" ORDER BY pv.version DESC) AS rank
    FROM public."projectVersion" pv
    JOIN public.project p ON p.id = pv."projectId"
)
INSERT INTO lake.lakedim_project (
    name,
    currentRequestedFunds,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
SELECT
    rp.name,
    rp."currentRequestedFunds",
    now() AS effectiveFrom,
    NULL AS effectiveTo,
    'HPC' AS source,
    rp.sourceId,
    'approved' AS dataStatus,
    now() AS createdAt
FROM ranked_projects rp
WHERE rp.rank = 1;

-- insert usage year
INSERT INTO lake.lakedim_year (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
SELECT
    uy."year"::TEXT AS name,
    now() AS effectiveFrom,
    NULL AS effectiveTo,
    'HPC' AS source,
    uy.id::VARCHAR AS sourceId,
    'approved' AS dataStatus,
    now() AS createdAt
FROM public."usageYear" uy;

-- insert emergency
INSERT INTO lake.lakedim_emergency (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
SELECT
    e.name,
    now() AS effectiveFrom,
    NULL AS effectiveTo,
    'HPC' AS source,
    e.id::VARCHAR AS sourceId,
    CASE WHEN e.active THEN 'approved' ELSE 'deprecated' END AS dataStatus,
    now() AS createdAt
FROM public.emergency e;

-- insert locations
INSERT INTO lake.lakedim_location (
    name,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    adminLevel,
    parentId,
    dataStatus,
    createdAt
)
SELECT
    l.name,
    NOW(),
    NULL,
    'HPC',
    l."externalId",
    l."adminLevel",
    l."parentId",
    'approved',
    NOW()
FROM public.location l
WHERE l."externalId" IS NOT NULL
  AND l.name IS NOT NULL;
  

-- insert metrictypes
INSERT INTO lake.lakedim_metrictype (
    name,
    description,
    effectivefrom,
    effectiveto,
    source,
    sourceid,
    datastatus,
    createdat
)
VALUES
    ('inNeed', 'People in need', NOW(), NULL, 'HPC', NULL, 'approved', NOW()),
    ('targeted', 'People targeted for assistance', NOW(), NULL, 'HPC', NULL, 'approved', NOW()),
    ('reached', 'People reached with assistance', NOW(), NULL, 'HPC', NULL, 'approved', NOW());


-- insert type
INSERT INTO lake.lakedim_caseloadtype (
    name,
    description,
    effectiveFrom,
    effectiveTo,
    source,
    sourceId,
    dataStatus,
    createdAt
)
VALUES
    ('caseLoad', 'Case Load', NOW(), NULL, 'HPC', NULL, 'approved', NOW()),
    ('textWebContent', 'Text Web Content', NOW(), NULL, 'HPC', NULL, 'approved', NOW()),
    ('indicator', 'Indicator Metric', NOW(), NULL, 'HPC', NULL, 'approved', NOW()),
    ('contact', 'Contact Information', NOW(), NULL, 'HPC', NULL, 'approved', NOW()),
    ('cost', 'Cost Data', NOW(), NULL, 'HPC', NULL, 'approved', NOW()),
    ('fileWebContent', 'File Web Content', NOW(), NULL, 'HPC', NULL, 'approved', NOW());