-- ==================================================
-- File:       hpc_lakehouse_schema_v1.sql
-- Purpose:    Define HPC lakehouse schema & tables
-- Author:     APMB/MATS - Sys&Dev Team
-- Created:    2025-04-10
-- Notes:      Executed in HPC db 'lake' schema
-- ==================================================

-- ***************************************************
-- Instructions to run the script
-- This file contains the Data Definition Language (DDL) for creating our lakehouse schema and all related tables.
-- This script assumes you're connected to a PostgreSQL database
-- Do not uncomment the CREATE SCHEMA lake AUTHORIZATION postgres; line. 
-- Instead, copy and run it manually in a separate query window if the lake schema does not already exist. 
-- After creating the schema, refresh your DB view.
-- Open a new query window inside the lake schema, and run the rest of the script.
-- Copy and execute all at once (you can also create all tables one by one).
-- This includes all dimension and fact tables necessary for the lakehouse.
-- ***************************************************


-- Do not uncommnet, copy/paste and run;
--CREATE SCHEMA lake AUTHORIZATION postgres;

-- Copy everthing from this point, paste and run inside 'lake' schema

-- Drop existing tables if needed
DROP TABLE IF EXISTS lake.lakefact_x CASCADE;
DROP TABLE IF EXISTS lake.lakedim_agegroup CASCADE;
DROP TABLE IF EXISTS lake.lakedim_demographic CASCADE;
DROP TABLE IF EXISTS lake.lakedim_emergency CASCADE;
DROP TABLE IF EXISTS lake.lakedim_fieldcluster CASCADE;
DROP TABLE IF EXISTS lake.lakedim_gender CASCADE;
DROP TABLE IF EXISTS lake.lakedim_globalcluster CASCADE;
DROP TABLE IF EXISTS lake.lakedim_location CASCADE;
DROP TABLE IF EXISTS lake.lakedim_organization CASCADE;
DROP TABLE IF EXISTS lake.lakedim_plan CASCADE;
DROP TABLE IF EXISTS lake.lakedim_populationtype CASCADE;
DROP TABLE IF EXISTS lake.lakedim_project CASCADE;
DROP TABLE IF EXISTS lake.lakedim_year CASCADE;
DROP TABLE IF EXISTS lake.lakedim_indicator CASCADE;
DROP TABLE IF EXISTS lakedim_caseloadtype CASCADE;
DROP TABLE IF EXISTS lake.lakedim_metricType CASCADE; 

-- Start creating tables.
CREATE TABLE lake.lakedim_agegroup (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_demographic (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_emergency (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_fieldcluster (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_gender (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_globalcluster (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_location (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    adminLevel INT,
    parentId INT,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_organization (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP, -- NULL = still active
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50), -- e.g. 'draft', 'approved', 'validated'
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);


CREATE TABLE lake.lakedim_plan (
    id SERIAL NOT NULL,
    name VARCHAR(1000) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_year (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_populationtype (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_project (
    id SERIAL NOT NULL,
    name VARCHAR(1000) NOT NULL,
    currentRequestedFunds BIGINT,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_indicator (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    unit VARCHAR(50),
    category VARCHAR(100),
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_caseloadtype (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakedim_metricType (
    id SERIAL NOT NULL,
    name VARCHAR(100) NOT NULL, -- e.g., 'InNeed', 'Targeted', 'Reached'
    description TEXT,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NULL,
    dataStatus VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id, effectiveFrom)
);

CREATE TABLE lake.lakefact_x (
    id SERIAL PRIMARY KEY,
    emergencyId INT,
    planId INT,
    organizationId INT,
    locationId INT,
    globalClusterId INT,
    fieldClusterId INT,
    projectId INT,
    yearId INT,
    populationTypeId INT,
    genderId INT,
    ageGroupId INT,
    demographicId INT,
    indicatorId INT,
    caseloadTypeId INT,
    metricTypeId INT,
    factType VARCHAR(50) NOT NULL,
    sourceType VARCHAR(50) NOT NULL,
    valueNum FLOAT8,
    valueStr VARCHAR(255),
    valueDate DATE,
    valueObject JSONB,
    effectiveFrom TIMESTAMP NOT NULL,
    effectiveTo TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    sourceId VARCHAR(50) NOT NULL,
    dataStatus VARCHAR(50),
    createdBy VARCHAR(50),
    createdAt TIMESTAMP DEFAULT now(),
    CONSTRAINT check_one_value CHECK (
        (
            (valueNum IS NOT NULL)::integer +
            (valueStr IS NOT NULL)::integer +
            (valueDate IS NOT NULL)::integer +
            (valueObject IS NOT NULL)::integer
        ) = 1
    ),
    CONSTRAINT unique_fact_grain UNIQUE (
        effectiveFrom, factType, sourceType,
        emergencyId, planId, organizationId, locationId,
        globalClusterId, fieldClusterId, projectId, yearId,
        populationTypeId, genderId, ageGroupId, demographicId,
        indicatorId, caseloadTypeId, metricTypeId
    )
);