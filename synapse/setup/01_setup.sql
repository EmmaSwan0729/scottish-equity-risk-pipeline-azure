-- Scottish Equity Risk Pipeline — Synapse Serverless SQL Setup
-- Run this script once when setting up the Synapse workspace.
-- Execute each section in order.

-- SECTION 1: Database and Schemas
CREATE DATABASE EquityRiskDB;

-- Switch connection to EquityRiskDB before running the rest of this script.

CREATE SCHEMA STAGING;
CREATE SCHEMA CORE;
CREATE SCHEMA MARTS;

-- SECTION 2: Master Key (required before creating credentials)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<REPLACE_WITH_STRONG_PASSWORD>';

-- SECTION 3: Managed Identity Credential
CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
WITH IDENTITY = 'Managed Identity';

-- SECTION 4: External Data Source (curated container)
CREATE EXTERNAL DATA SOURCE CuratedDataSource
WITH (
    LOCATION = 'https://stscotequityriskuk.dfs.core.windows.net/curated',
    CREDENTIAL = WorkspaceIdentity
);

-- SECTION 5: External File Format
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET
);