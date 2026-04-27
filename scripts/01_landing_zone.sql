/*
 * Script 01: Landing Zone Setup
 * Creates: database, schemas, internal stage, warehouse
 * Run as: ACCOUNTADMIN (or role with CREATE DATABASE)
 */

CREATE DATABASE IF NOT EXISTS AI_PLATFORM_DEV
  COMMENT = 'AI Platform lower environment for POD processing and conversational agent';

CREATE SCHEMA IF NOT EXISTS AI_PLATFORM_DEV.RAW
  COMMENT = 'Raw ingestion layer for POD documents and source data';

CREATE SCHEMA IF NOT EXISTS AI_PLATFORM_DEV.CURATED
  COMMENT = 'Curated/modeled layer - fact/dim tables for logistics';

CREATE SCHEMA IF NOT EXISTS AI_PLATFORM_DEV.SEMANTIC
  COMMENT = 'Semantic views, search services, and agents';

-- Internal stage for POD documents
-- For production: replace with EXTERNAL STAGE pointing to S3/ADLS/GCS
CREATE STAGE IF NOT EXISTS AI_PLATFORM_DEV.RAW.POD_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Internal stage for POD document ingestion (PDF/images)';

-- Warehouse for AI workloads (XS for DEV, size up for production)
CREATE WAREHOUSE IF NOT EXISTS AI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  COMMENT = 'AI workload warehouse for document processing, agent execution, and search services';
