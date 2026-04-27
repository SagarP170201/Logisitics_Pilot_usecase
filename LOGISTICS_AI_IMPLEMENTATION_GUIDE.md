# Logistics AI Implementation Guide

## POD Document Processing (Use Case 1) + Conversational Agent (Use Case 3)

**Snowflake-Native Implementation | Lower Environment First**

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Phase 1: Create AI Landing Zone](#2-phase-1-create-ai-landing-zone)
3. [Phase 2: Create Core Logistics Tables](#3-phase-2-create-core-logistics-tables)
4. [Phase 3: POD Document Ingestion Pipeline](#4-phase-3-pod-document-ingestion-pipeline)
5. [Phase 4: Cortex Search Service for PODs](#5-phase-4-cortex-search-service-for-pods)
6. [Phase 5: Semantic View for Cortex Analyst](#6-phase-5-semantic-view-for-cortex-analyst)
7. [Phase 6: Cortex Agent (Conversational Interface)](#7-phase-6-cortex-agent)
8. [Phase 7: Testing & Validation](#8-phase-7-testing--validation)
9. [Production Promotion Checklist](#9-production-promotion-checklist)
10. [Architecture Notes for Use Case 2](#10-architecture-notes-for-use-case-2)

---

## 1. Prerequisites

### Account Requirements

- Snowflake account with **Cortex AI** features enabled (Cortex Agent, Cortex Analyst, Cortex Search, AI Functions)
- Region: Cortex features are available in most AWS/Azure regions. Verify your region supports `AI_PARSE_DOCUMENT`, `AI_EXTRACT`, and Cortex Agents
- The role you use must have the database role `SNOWFLAKE.CORTEX_USER` granted

### Role & Privileges

For lower environment, you can use `ACCOUNTADMIN` or `SYSADMIN`. For production, create a dedicated role:

```sql
-- Production role setup (do this later, not for DEV)
CREATE ROLE IF NOT EXISTS AI_APP_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_APP_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE AI_APP_ROLE;
-- ... additional grants as needed
```

### What You Will Build

| Component | Snowflake Feature | Purpose |
|---|---|---|
| POD ingestion pipeline | `AI_PARSE_DOCUMENT` + `AI_EXTRACT` + Streams/Tasks | Auto-extract structured data from POD PDFs |
| POD semantic search | Cortex Search Service | Find specific POD docs by natural language |
| Logistics analytics | Semantic View + Cortex Analyst | Text-to-SQL for KPIs (on-time %, claims, etc.) |
| Conversational agent | Cortex Agent | Unified NL interface over structured + unstructured data |

---

## 2. Phase 1: Create AI Landing Zone

Create the database, schemas, stage, and warehouse.

```sql
--------------------------------------------------------------------
-- 1.1 Database
--------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS AI_PLATFORM_DEV
  COMMENT = 'AI Platform lower environment for POD processing and conversational agent';

--------------------------------------------------------------------
-- 1.2 Schemas
--------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS AI_PLATFORM_DEV.RAW
  COMMENT = 'Raw ingestion layer for POD documents and source data';

CREATE SCHEMA IF NOT EXISTS AI_PLATFORM_DEV.CURATED
  COMMENT = 'Curated/modeled layer - fact/dim tables for logistics';

CREATE SCHEMA IF NOT EXISTS AI_PLATFORM_DEV.SEMANTIC
  COMMENT = 'Semantic views, search services, and agents';

--------------------------------------------------------------------
-- 1.3 Internal Stage for POD Documents
--     For production: replace with an EXTERNAL STAGE pointing to
--     your S3/ADLS/GCS bucket where POD files land.
--------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS AI_PLATFORM_DEV.RAW.POD_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Internal stage for POD document ingestion (PDF/images)';

--------------------------------------------------------------------
-- 1.4 Warehouse for AI Workloads
--     XS is fine for DEV. Size up for production volumes.
--------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS AI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  COMMENT = 'AI workload warehouse for document processing, agent execution, and search services';
```

**Checkpoint:** You should now have `AI_PLATFORM_DEV` with 3 schemas, a stage, and a warehouse.

---

## 3. Phase 2: Create Core Logistics Tables

These are the curated fact/dimension tables that both the semantic view and search service will use. **Replace this sample data with your actual production tables.**

### 3.1 Dimension Tables

```sql
--------------------------------------------------------------------
-- Carriers
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.CURATED.DIM_CARRIERS (
    CARRIER_ID VARCHAR(20) PRIMARY KEY,
    CARRIER_NAME VARCHAR(200),
    CARRIER_TYPE VARCHAR(50),
    SERVICE_REGION VARCHAR(100),
    ON_TIME_RATING FLOAT,
    ACTIVE_FLAG BOOLEAN DEFAULT TRUE
);

-- >>> INSERT your carrier data here <<<
-- Sample:
INSERT INTO AI_PLATFORM_DEV.CURATED.DIM_CARRIERS VALUES
('CAR001', 'BlueDart Express', 'Express', 'Pan-India', 0.92, TRUE),
('CAR002', 'Delhivery', 'Standard', 'Pan-India', 0.87, TRUE),
('CAR003', 'DTDC Courier', 'Economy', 'Pan-India', 0.81, TRUE),
('CAR004', 'Gati Ltd', 'Freight', 'North & West India', 0.85, TRUE),
('CAR005', 'Rivigo', 'Long-Haul', 'Pan-India', 0.89, TRUE),
('CAR006', 'Ecom Express', 'E-commerce', 'Metro Cities', 0.90, TRUE),
('CAR007', 'Shadowfax', 'Last-Mile', 'Metro Cities', 0.88, TRUE),
('CAR008', 'XpressBees', 'Express', 'Pan-India', 0.86, TRUE);

--------------------------------------------------------------------
-- Customers
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.CURATED.DIM_CUSTOMERS (
    CUSTOMER_ID VARCHAR(20) PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(200),
    CUSTOMER_SEGMENT VARCHAR(50),
    CITY VARCHAR(100),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50),
    CONTACT_EMAIL VARCHAR(200),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- >>> INSERT your customer data here <<<
-- Sample:
INSERT INTO AI_PLATFORM_DEV.CURATED.DIM_CUSTOMERS VALUES
('CUST001', 'Acme Corp', 'Enterprise', 'Mumbai', 'Maharashtra', 'India', 'logistics@acme.com', '2024-01-15'),
('CUST002', 'Beta Industries', 'Mid-Market', 'Chennai', 'Tamil Nadu', 'India', 'ops@beta.in', '2024-02-10'),
('CUST003', 'Gamma Retail', 'SMB', 'Delhi', 'Delhi', 'India', 'supply@gamma.in', '2024-03-01'),
('CUST004', 'Delta Pharma', 'Enterprise', 'Hyderabad', 'Telangana', 'India', 'dist@deltapharma.com', '2024-04-20'),
('CUST005', 'Epsilon Electronics', 'Mid-Market', 'Bangalore', 'Karnataka', 'India', 'fulfillment@epsilon.in', '2024-05-12');
```

### 3.2 Fact Tables

```sql
--------------------------------------------------------------------
-- Shipments
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.CURATED.FACT_SHIPMENTS (
    SHIPMENT_ID VARCHAR(20) PRIMARY KEY,
    ORDER_ID VARCHAR(20),
    CUSTOMER_ID VARCHAR(20),
    CARRIER_ID VARCHAR(20),
    ORIGIN_CITY VARCHAR(100),
    DESTINATION_CITY VARCHAR(100),
    SHIP_DATE DATE,
    EXPECTED_DELIVERY_DATE DATE,
    ACTUAL_DELIVERY_DATE DATE,
    STATUS VARCHAR(50),
    WEIGHT_KG FLOAT,
    NUM_PACKAGES INT,
    FREIGHT_COST FLOAT,
    IS_ON_TIME BOOLEAN,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- >>> Load your shipment data here (COPY INTO from stage, or INSERT from source tables) <<<

--------------------------------------------------------------------
-- Orders
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.CURATED.FACT_ORDERS (
    ORDER_ID VARCHAR(20) PRIMARY KEY,
    CUSTOMER_ID VARCHAR(20),
    ORDER_DATE DATE,
    ORDER_VALUE FLOAT,
    ORDER_STATUS VARCHAR(50),
    PAYMENT_METHOD VARCHAR(50),
    NUM_ITEMS INT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- >>> Load your order data here <<<

--------------------------------------------------------------------
-- POD Fact (extracted POD data - will be populated by the pipeline)
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.CURATED.POD_FACT (
    POD_ID VARCHAR(20) PRIMARY KEY,
    SHIPMENT_ID VARCHAR(20),
    ORDER_ID VARCHAR(20),
    CUSTOMER_ID VARCHAR(20),
    CARRIER_ID VARCHAR(20),
    DELIVERY_DATE DATE,
    SIGNED_BY VARCHAR(200),
    RECEIVER_NAME VARCHAR(200),
    DELIVERY_ADDRESS VARCHAR(500),
    DELIVERY_CITY VARCHAR(100),
    DELIVERY_STATE VARCHAR(50),
    POD_STATUS VARCHAR(50),
    EXCEPTION_FLAG BOOLEAN DEFAULT FALSE,
    EXCEPTION_NOTES VARCHAR(1000),
    SIGNATURE_PRESENT BOOLEAN DEFAULT TRUE,
    DAMAGE_REPORTED BOOLEAN DEFAULT FALSE,
    DAMAGE_DESCRIPTION VARCHAR(500),
    PARTIAL_DELIVERY BOOLEAN DEFAULT FALSE,
    PACKAGES_RECEIVED INT,
    PACKAGES_EXPECTED INT,
    POD_FILE_NAME VARCHAR(500),
    POD_TEXT_CONTENT VARCHAR(16000),
    EXTRACTION_CONFIDENCE FLOAT,
    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--------------------------------------------------------------------
-- Claims
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.CURATED.FACT_CLAIMS (
    CLAIM_ID VARCHAR(20) PRIMARY KEY,
    SHIPMENT_ID VARCHAR(20),
    POD_ID VARCHAR(20),
    CUSTOMER_ID VARCHAR(20),
    CARRIER_ID VARCHAR(20),
    CLAIM_DATE DATE,
    CLAIM_TYPE VARCHAR(50),
    CLAIM_AMOUNT FLOAT,
    CLAIM_STATUS VARCHAR(50),
    RESOLUTION_DATE DATE,
    RESOLUTION_AMOUNT FLOAT,
    DESCRIPTION VARCHAR(1000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- >>> Load your claims data here <<<
```

**Checkpoint:** All tables created. Load your actual data (or use sample generators for DEV testing).

---

## 4. Phase 3: POD Document Ingestion Pipeline

This sets up the automated flow: **POD PDF lands in stage -> AI extracts text -> AI extracts structured fields -> writes to POD_FACT**.

### 4.1 Raw File Registry + Streams

```sql
--------------------------------------------------------------------
-- Table to track files landed in POD_STAGE
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.RAW.POD_FILES (
    FILE_NAME VARCHAR(500),
    FILE_RELATIVE_PATH VARCHAR(1000),
    FILE_TYPE VARCHAR(20),
    FILE_SIZE NUMBER,
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSED BOOLEAN DEFAULT FALSE
);

CREATE OR REPLACE STREAM AI_PLATFORM_DEV.RAW.POD_FILES_STREAM
  ON TABLE AI_PLATFORM_DEV.RAW.POD_FILES
  APPEND_ONLY = TRUE;

--------------------------------------------------------------------
-- Table for parsed document text
--------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.RAW.POD_EXTRACTED (
    FILE_NAME VARCHAR(500),
    FILE_RELATIVE_PATH VARCHAR(1000),
    RAW_TEXT VARCHAR(16777216),
    PAGE_COUNT INT,
    EXTRACTION_MODE VARCHAR(20),
    EXTRACTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ERROR_INFO VARCHAR(2000)
);

CREATE OR REPLACE STREAM AI_PLATFORM_DEV.RAW.POD_EXTRACTED_STREAM
  ON TABLE AI_PLATFORM_DEV.RAW.POD_EXTRACTED
  APPEND_ONLY = TRUE;
```

### 4.2 Task 1: Parse Documents (AI_PARSE_DOCUMENT)

This task fires when new files appear in the stream and uses `AI_PARSE_DOCUMENT` in LAYOUT mode to extract text while preserving tables and structure.

> **Cost note:** LAYOUT mode costs ~3.33 credits per 1,000 pages. OCR mode is ~0.5 credits per 1,000 pages but loses table structure.

```sql
CREATE OR REPLACE TASK AI_PLATFORM_DEV.RAW.POD_EXTRACTION_TASK
  WAREHOUSE = AI_WH
  SCHEDULE = '5 MINUTE'
  COMMENT = 'Parses new POD files using AI_PARSE_DOCUMENT (LAYOUT mode)'
  WHEN SYSTEM$STREAM_HAS_DATA('AI_PLATFORM_DEV.RAW.POD_FILES_STREAM')
AS
INSERT INTO AI_PLATFORM_DEV.RAW.POD_EXTRACTED
  (FILE_NAME, FILE_RELATIVE_PATH, RAW_TEXT, PAGE_COUNT, EXTRACTION_MODE, ERROR_INFO)
SELECT
    s.FILE_NAME,
    s.FILE_RELATIVE_PATH,
    parsed:content::STRING AS RAW_TEXT,
    parsed:metadata:pageCount::INT AS PAGE_COUNT,
    'LAYOUT' AS EXTRACTION_MODE,
    parsed:errorInformation::STRING AS ERROR_INFO
FROM AI_PLATFORM_DEV.RAW.POD_FILES_STREAM s,
  LATERAL (
    SELECT AI_PARSE_DOCUMENT(
      TO_FILE('@AI_PLATFORM_DEV.RAW.POD_STAGE', s.FILE_RELATIVE_PATH),
      {'mode': 'LAYOUT'}
    ) AS parsed
  );
```

### 4.3 Task 2: Extract Structured Fields (AI_EXTRACT)

This child task takes parsed text and pulls out structured POD fields using `AI_EXTRACT`.

```sql
CREATE OR REPLACE TASK AI_PLATFORM_DEV.RAW.POD_FIELD_EXTRACTION_TASK
  WAREHOUSE = AI_WH
  COMMENT = 'Extracts structured fields from parsed POD text into CURATED.POD_FACT'
  AFTER AI_PLATFORM_DEV.RAW.POD_EXTRACTION_TASK
AS
INSERT INTO AI_PLATFORM_DEV.CURATED.POD_FACT (
    POD_ID, SHIPMENT_ID, ORDER_ID, CUSTOMER_ID, CARRIER_ID,
    DELIVERY_DATE, SIGNED_BY, RECEIVER_NAME, DELIVERY_ADDRESS,
    DELIVERY_CITY, DELIVERY_STATE, POD_STATUS, EXCEPTION_FLAG,
    EXCEPTION_NOTES, SIGNATURE_PRESENT, DAMAGE_REPORTED,
    DAMAGE_DESCRIPTION, PARTIAL_DELIVERY, PACKAGES_RECEIVED,
    PACKAGES_EXPECTED, POD_FILE_NAME, POD_TEXT_CONTENT, EXTRACTION_CONFIDENCE
)
SELECT
    fields:pod_reference::VARCHAR,
    fields:shipment_id::VARCHAR,
    fields:order_id::VARCHAR,
    fields:customer_id::VARCHAR,
    fields:carrier_name::VARCHAR,
    TRY_TO_DATE(fields:delivery_date::VARCHAR),
    fields:signed_by::VARCHAR,
    fields:receiver_name::VARCHAR,
    fields:delivery_address::VARCHAR,
    fields:delivery_city::VARCHAR,
    fields:delivery_state::VARCHAR,
    fields:delivery_status::VARCHAR,
    CASE WHEN fields:exception_notes::VARCHAR IS NOT NULL
          AND fields:exception_notes::VARCHAR != ''
         THEN TRUE ELSE FALSE END,
    fields:exception_notes::VARCHAR,
    CASE WHEN fields:signature_present::VARCHAR ILIKE 'yes%'
           OR fields:signature_present::VARCHAR ILIKE 'true%'
         THEN TRUE ELSE FALSE END,
    CASE WHEN fields:damage_description::VARCHAR IS NOT NULL
          AND fields:damage_description::VARCHAR != ''
         THEN TRUE ELSE FALSE END,
    fields:damage_description::VARCHAR,
    CASE WHEN fields:partial_delivery::VARCHAR ILIKE 'yes%'
           OR fields:partial_delivery::VARCHAR ILIKE 'true%'
         THEN TRUE ELSE FALSE END,
    TRY_TO_NUMBER(fields:packages_received::VARCHAR),
    TRY_TO_NUMBER(fields:packages_expected::VARCHAR),
    s.FILE_NAME,
    s.RAW_TEXT,
    0.85
FROM AI_PLATFORM_DEV.RAW.POD_EXTRACTED_STREAM s,
  LATERAL (
    SELECT AI_EXTRACT(
      s.RAW_TEXT,
      OBJECT_CONSTRUCT(
        'pod_reference', 'POD reference ID',
        'shipment_id', 'Shipment or tracking ID',
        'order_id', 'Order ID',
        'customer_id', 'Customer ID or account number',
        'carrier_name', 'Carrier or courier company name',
        'delivery_date', 'Date of delivery (YYYY-MM-DD)',
        'signed_by', 'Person who signed for delivery',
        'receiver_name', 'Name or title of receiver',
        'delivery_address', 'Full delivery address',
        'delivery_city', 'City of delivery',
        'delivery_state', 'State of delivery',
        'delivery_status', 'Delivery status',
        'exception_notes', 'Any exception or damage notes',
        'signature_present', 'Whether signature is present (Yes/No)',
        'damage_description', 'Description of any damage',
        'partial_delivery', 'Whether partial delivery (Yes/No)',
        'packages_received', 'Number of packages received',
        'packages_expected', 'Number of packages expected'
      )
    ) AS fields
  );
```

### 4.4 Resume Tasks (When Ready to Process)

Tasks are created in SUSPENDED state. Resume them when you have POD files in the stage:

```sql
-- Resume tasks to start processing
ALTER TASK AI_PLATFORM_DEV.RAW.POD_FIELD_EXTRACTION_TASK RESUME;
ALTER TASK AI_PLATFORM_DEV.RAW.POD_EXTRACTION_TASK RESUME;

-- To test: upload a sample POD PDF and register it
-- PUT file:///path/to/sample_pod.pdf @AI_PLATFORM_DEV.RAW.POD_STAGE;
-- ALTER STAGE AI_PLATFORM_DEV.RAW.POD_STAGE REFRESH;
-- INSERT INTO AI_PLATFORM_DEV.RAW.POD_FILES (FILE_NAME, FILE_RELATIVE_PATH, FILE_TYPE)
--   SELECT RELATIVE_PATH, RELATIVE_PATH, 'PDF'
--   FROM DIRECTORY(@AI_PLATFORM_DEV.RAW.POD_STAGE);
```

**Checkpoint:** Pipeline is wired. Upload a test PDF to validate extraction quality before scaling.

---

## 5. Phase 4: Cortex Search Service for PODs

Creates a semantic search index over POD documents so the agent can find specific PODs by natural language.

### 5.1 Search Corpus (Denormalized View)

```sql
CREATE OR REPLACE TABLE AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_CORPUS AS
SELECT
    p.POD_ID,
    p.SHIPMENT_ID,
    p.ORDER_ID,
    c.CUSTOMER_NAME,
    c.CUSTOMER_SEGMENT,
    cr.CARRIER_NAME,
    cr.CARRIER_TYPE,
    p.DELIVERY_DATE,
    p.DELIVERY_CITY,
    p.DELIVERY_STATE,
    p.POD_STATUS,
    p.SIGNED_BY,
    p.EXCEPTION_FLAG,
    p.EXCEPTION_NOTES,
    p.DAMAGE_REPORTED,
    p.DAMAGE_DESCRIPTION,
    p.SIGNATURE_PRESENT,
    p.PARTIAL_DELIVERY,
    p.PACKAGES_RECEIVED,
    p.PACKAGES_EXPECTED,
    p.POD_TEXT_CONTENT,
    -- Enriched search text combining document + metadata
    COALESCE(p.POD_TEXT_CONTENT, '') || CHR(10) || CHR(10) ||
    'Customer: ' || COALESCE(c.CUSTOMER_NAME, 'Unknown') || CHR(10) ||
    'Carrier: ' || COALESCE(cr.CARRIER_NAME, 'Unknown') || CHR(10) ||
    'Status: ' || COALESCE(p.POD_STATUS, 'Unknown') || CHR(10) ||
    'City: ' || COALESCE(p.DELIVERY_CITY, 'Unknown') || CHR(10) ||
    CASE WHEN p.EXCEPTION_FLAG
         THEN 'EXCEPTION: ' || COALESCE(p.EXCEPTION_NOTES, 'No details') || CHR(10)
         ELSE '' END ||
    CASE WHEN p.DAMAGE_REPORTED
         THEN 'DAMAGE: ' || COALESCE(p.DAMAGE_DESCRIPTION, 'No details') || CHR(10)
         ELSE '' END ||
    CASE WHEN NOT p.SIGNATURE_PRESENT
         THEN 'WARNING: Missing signature' || CHR(10)
         ELSE '' END
    AS SEARCH_TEXT
FROM AI_PLATFORM_DEV.CURATED.POD_FACT p
LEFT JOIN AI_PLATFORM_DEV.CURATED.DIM_CUSTOMERS c ON p.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN AI_PLATFORM_DEV.CURATED.DIM_CARRIERS cr ON p.CARRIER_ID = cr.CARRIER_ID;
```

### 5.2 Create Search Service

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_SERVICE
  ON SEARCH_TEXT
  ATTRIBUTES POD_ID, SHIPMENT_ID, ORDER_ID, CUSTOMER_NAME, CARRIER_NAME,
             DELIVERY_DATE, DELIVERY_CITY, POD_STATUS, EXCEPTION_FLAG,
             EXCEPTION_NOTES, DAMAGE_REPORTED, SIGNATURE_PRESENT
  WAREHOUSE = AI_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Semantic search over POD documents'
AS
  SELECT
    SEARCH_TEXT,
    POD_ID,
    SHIPMENT_ID,
    ORDER_ID,
    CUSTOMER_NAME,
    CARRIER_NAME,
    DELIVERY_DATE::VARCHAR AS DELIVERY_DATE,
    DELIVERY_CITY,
    POD_STATUS,
    EXCEPTION_FLAG::VARCHAR AS EXCEPTION_FLAG,
    EXCEPTION_NOTES,
    DAMAGE_REPORTED::VARCHAR AS DAMAGE_REPORTED,
    SIGNATURE_PRESENT::VARCHAR AS SIGNATURE_PRESENT
  FROM AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_CORPUS;
```

> **Important:** Cortex Search requires VARCHAR columns. Cast BOOLEAN and DATE types to VARCHAR in the SELECT.

> **Refresh:** The search corpus table is static. To keep it fresh, either:
> - Recreate it periodically (simple), or
> - Replace the table with a VIEW or DYNAMIC TABLE pointing at the live POD_FACT + dim tables.

**Checkpoint:** Run `SHOW CORTEX SEARCH SERVICES IN SCHEMA AI_PLATFORM_DEV.SEMANTIC;` to confirm the service is `ACTIVE`.

---

## 6. Phase 5: Semantic View for Cortex Analyst

This creates the semantic model that powers the text-to-SQL tool in the agent.

```sql
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'AI_PLATFORM_DEV.SEMANTIC',
  $$
name: LOGISTICS_OPS_ANALYTICS
description: Logistics operations analytics covering shipments, orders, POD documents, claims, customers, and carriers.
tables:
  - name: SHIPMENTS
    description: Shipment records with origin, destination, carrier, delivery status, and cost.
    base_table:
      database: AI_PLATFORM_DEV
      schema: CURATED
      table: FACT_SHIPMENTS
    primary_key:
      columns:
        - SHIPMENT_ID
    dimensions:
      - name: SHIPMENT_ID
        expr: SHIPMENT_ID
        data_type: TEXT
        description: Unique shipment identifier
      - name: ORDER_ID
        expr: ORDER_ID
        data_type: TEXT
        description: Associated order identifier
      - name: CUSTOMER_ID
        expr: CUSTOMER_ID
        data_type: TEXT
        description: Customer who placed the shipment
      - name: CARRIER_ID
        expr: CARRIER_ID
        data_type: TEXT
        description: Carrier handling the shipment
      - name: ORIGIN_CITY
        expr: ORIGIN_CITY
        data_type: TEXT
        description: City where the shipment originated
      - name: DESTINATION_CITY
        expr: DESTINATION_CITY
        data_type: TEXT
        description: City where the shipment is being delivered
      - name: STATUS
        expr: STATUS
        data_type: TEXT
        description: Current shipment status
        sample_values: [Delivered, In Transit, Out for Delivery, Exception, Returned]
      - name: IS_ON_TIME
        expr: IS_ON_TIME
        data_type: BOOLEAN
        description: Whether the shipment was delivered on time
      - name: SHIP_DATE
        expr: SHIP_DATE
        data_type: DATE
        description: Date when shipment was dispatched
      - name: EXPECTED_DELIVERY_DATE
        expr: EXPECTED_DELIVERY_DATE
        data_type: DATE
        description: Expected delivery date
      - name: ACTUAL_DELIVERY_DATE
        expr: ACTUAL_DELIVERY_DATE
        data_type: DATE
        description: Actual delivery date
    facts:
      - name: WEIGHT_KG
        expr: WEIGHT_KG
        data_type: NUMBER
        description: Weight in kilograms
      - name: NUM_PACKAGES
        expr: NUM_PACKAGES
        data_type: NUMBER
        description: Number of packages
      - name: FREIGHT_COST
        expr: FREIGHT_COST
        data_type: NUMBER
        description: Total freight cost
    metrics:
      - name: TOTAL_SHIPMENTS
        expr: COUNT(SHIPMENT_ID)
        description: Total shipment count
      - name: TOTAL_FREIGHT_COST
        expr: SUM(FREIGHT_COST)
        description: Total freight cost
      - name: ON_TIME_DELIVERY_RATE
        expr: "ROUND(100.0 * SUM(CASE WHEN IS_ON_TIME THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)"
        description: On-time delivery percentage
  - name: ORDERS
    description: Customer orders with value, status, and payment details.
    base_table:
      database: AI_PLATFORM_DEV
      schema: CURATED
      table: FACT_ORDERS
    primary_key:
      columns:
        - ORDER_ID
    dimensions:
      - name: ORDER_ID
        expr: ORDER_ID
        data_type: TEXT
        description: Unique order identifier
      - name: CUSTOMER_ID
        expr: CUSTOMER_ID
        data_type: TEXT
        description: Customer who placed the order
      - name: ORDER_STATUS
        expr: ORDER_STATUS
        data_type: TEXT
        description: Current order status
        sample_values: [Fulfilled, Processing, Cancelled, Returned]
      - name: PAYMENT_METHOD
        expr: PAYMENT_METHOD
        data_type: TEXT
        description: Payment method used
      - name: ORDER_DATE
        expr: ORDER_DATE
        data_type: DATE
        description: Date order was placed
    facts:
      - name: ORDER_VALUE
        expr: ORDER_VALUE
        data_type: NUMBER
        description: Monetary value of the order
      - name: NUM_ITEMS
        expr: NUM_ITEMS
        data_type: NUMBER
        description: Number of items
    metrics:
      - name: TOTAL_ORDERS
        expr: COUNT(ORDER_ID)
        description: Total order count
      - name: TOTAL_ORDER_VALUE
        expr: SUM(ORDER_VALUE)
        description: Total order value
  - name: POD_DOCUMENTS
    description: Proof of Delivery documents with delivery details, exceptions, signature status, and damage records.
    base_table:
      database: AI_PLATFORM_DEV
      schema: CURATED
      table: POD_FACT
    primary_key:
      columns:
        - POD_ID
    dimensions:
      - name: POD_ID
        expr: POD_ID
        data_type: TEXT
        description: Unique POD identifier
      - name: SHIPMENT_ID
        expr: SHIPMENT_ID
        data_type: TEXT
        description: Associated shipment
      - name: CUSTOMER_ID
        expr: CUSTOMER_ID
        data_type: TEXT
        description: Customer associated with this POD
      - name: CARRIER_ID
        expr: CARRIER_ID
        data_type: TEXT
        description: Carrier that performed delivery
      - name: SIGNED_BY
        expr: SIGNED_BY
        data_type: TEXT
        description: Person who signed for delivery
      - name: DELIVERY_CITY
        expr: DELIVERY_CITY
        data_type: TEXT
        description: City where delivery was made
      - name: POD_STATUS
        expr: POD_STATUS
        data_type: TEXT
        description: POD delivery status
        sample_values: [Delivered - Clean, Delivered - With Exceptions, Partial Delivery, Refused]
      - name: EXCEPTION_FLAG
        expr: EXCEPTION_FLAG
        data_type: BOOLEAN
        description: Whether this POD has exceptions
      - name: EXCEPTION_NOTES
        expr: EXCEPTION_NOTES
        data_type: TEXT
        description: Description of delivery exceptions
      - name: SIGNATURE_PRESENT
        expr: SIGNATURE_PRESENT
        data_type: BOOLEAN
        description: Whether signature was obtained
      - name: DAMAGE_REPORTED
        expr: DAMAGE_REPORTED
        data_type: BOOLEAN
        description: Whether damage was reported
      - name: PARTIAL_DELIVERY
        expr: PARTIAL_DELIVERY
        data_type: BOOLEAN
        description: Whether this was a partial delivery
      - name: DELIVERY_DATE
        expr: DELIVERY_DATE
        data_type: DATE
        description: Date delivery was completed
    facts:
      - name: PACKAGES_RECEIVED
        expr: PACKAGES_RECEIVED
        data_type: NUMBER
        description: Packages actually received
      - name: PACKAGES_EXPECTED
        expr: PACKAGES_EXPECTED
        data_type: NUMBER
        description: Packages expected
    metrics:
      - name: TOTAL_PODS
        expr: COUNT(POD_ID)
        description: Total POD document count
      - name: EXCEPTION_RATE
        expr: "ROUND(100.0 * SUM(CASE WHEN EXCEPTION_FLAG THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)"
        description: POD exception percentage
      - name: MISSING_SIGNATURE_COUNT
        expr: "SUM(CASE WHEN NOT SIGNATURE_PRESENT THEN 1 ELSE 0 END)"
        description: PODs without signature
      - name: DAMAGE_RATE
        expr: "ROUND(100.0 * SUM(CASE WHEN DAMAGE_REPORTED THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)"
        description: POD damage percentage
  - name: CLAIMS
    description: Logistics claims with type, amount, status, and resolution details.
    base_table:
      database: AI_PLATFORM_DEV
      schema: CURATED
      table: FACT_CLAIMS
    primary_key:
      columns:
        - CLAIM_ID
    dimensions:
      - name: CLAIM_ID
        expr: CLAIM_ID
        data_type: TEXT
        description: Unique claim identifier
      - name: SHIPMENT_ID
        expr: SHIPMENT_ID
        data_type: TEXT
        description: Shipment associated with claim
      - name: CUSTOMER_ID
        expr: CUSTOMER_ID
        data_type: TEXT
        description: Customer who filed the claim
      - name: CARRIER_ID
        expr: CARRIER_ID
        data_type: TEXT
        description: Carrier against whom claim was filed
      - name: CLAIM_TYPE
        expr: CLAIM_TYPE
        data_type: TEXT
        description: Type of claim
        sample_values: [Damage, Loss, Shortage, Late Delivery]
      - name: CLAIM_STATUS
        expr: CLAIM_STATUS
        data_type: TEXT
        description: Current claim status
        sample_values: [Resolved, Under Review, Pending Documentation, Rejected]
      - name: CLAIM_DATE
        expr: CLAIM_DATE
        data_type: DATE
        description: Date claim was filed
      - name: RESOLUTION_DATE
        expr: RESOLUTION_DATE
        data_type: DATE
        description: Date claim was resolved
    facts:
      - name: CLAIM_AMOUNT
        expr: CLAIM_AMOUNT
        data_type: NUMBER
        description: Amount claimed
      - name: RESOLUTION_AMOUNT
        expr: RESOLUTION_AMOUNT
        data_type: NUMBER
        description: Amount resolved or paid
    metrics:
      - name: TOTAL_CLAIMS
        expr: COUNT(CLAIM_ID)
        description: Total claims filed
      - name: TOTAL_CLAIM_AMOUNT
        expr: SUM(CLAIM_AMOUNT)
        description: Total claim value
  - name: CUSTOMERS
    description: Customer dimension with names, segments, and locations.
    base_table:
      database: AI_PLATFORM_DEV
      schema: CURATED
      table: DIM_CUSTOMERS
    primary_key:
      columns:
        - CUSTOMER_ID
    dimensions:
      - name: CUSTOMER_ID
        expr: CUSTOMER_ID
        data_type: TEXT
        description: Unique customer identifier
      - name: CUSTOMER_NAME
        expr: CUSTOMER_NAME
        data_type: TEXT
        description: Customer organization name
      - name: CUSTOMER_SEGMENT
        expr: CUSTOMER_SEGMENT
        data_type: TEXT
        description: Customer segment
        sample_values: [Enterprise, Mid-Market, SMB]
      - name: CITY
        expr: CITY
        data_type: TEXT
        description: Customer city
      - name: STATE
        expr: STATE
        data_type: TEXT
        description: Customer state
  - name: CARRIERS
    description: Carrier dimension with names, types, and service regions.
    base_table:
      database: AI_PLATFORM_DEV
      schema: CURATED
      table: DIM_CARRIERS
    primary_key:
      columns:
        - CARRIER_ID
    dimensions:
      - name: CARRIER_ID
        expr: CARRIER_ID
        data_type: TEXT
        description: Unique carrier identifier
      - name: CARRIER_NAME
        expr: CARRIER_NAME
        data_type: TEXT
        description: Carrier company name
      - name: CARRIER_TYPE
        expr: CARRIER_TYPE
        data_type: TEXT
        description: Type of carrier service
      - name: SERVICE_REGION
        expr: SERVICE_REGION
        data_type: TEXT
        description: Geographic region served
    facts:
      - name: ON_TIME_RATING
        expr: ON_TIME_RATING
        data_type: NUMBER
        description: Historical on-time delivery rating (0-1)
relationships:
  - name: SHIPMENTS_TO_CUSTOMERS
    left_table: SHIPMENTS
    right_table: CUSTOMERS
    relationship_columns:
      - left_column: CUSTOMER_ID
        right_column: CUSTOMER_ID
    relationship_type: many_to_one
  - name: SHIPMENTS_TO_CARRIERS
    left_table: SHIPMENTS
    right_table: CARRIERS
    relationship_columns:
      - left_column: CARRIER_ID
        right_column: CARRIER_ID
    relationship_type: many_to_one
  - name: ORDERS_TO_CUSTOMERS
    left_table: ORDERS
    right_table: CUSTOMERS
    relationship_columns:
      - left_column: CUSTOMER_ID
        right_column: CUSTOMER_ID
    relationship_type: many_to_one
  - name: POD_TO_CUSTOMERS
    left_table: POD_DOCUMENTS
    right_table: CUSTOMERS
    relationship_columns:
      - left_column: CUSTOMER_ID
        right_column: CUSTOMER_ID
    relationship_type: many_to_one
  - name: POD_TO_CARRIERS
    left_table: POD_DOCUMENTS
    right_table: CARRIERS
    relationship_columns:
      - left_column: CARRIER_ID
        right_column: CARRIER_ID
    relationship_type: many_to_one
  - name: CLAIMS_TO_CUSTOMERS
    left_table: CLAIMS
    right_table: CUSTOMERS
    relationship_columns:
      - left_column: CUSTOMER_ID
        right_column: CUSTOMER_ID
    relationship_type: many_to_one
  - name: CLAIMS_TO_CARRIERS
    left_table: CLAIMS
    right_table: CARRIERS
    relationship_columns:
      - left_column: CARRIER_ID
        right_column: CARRIER_ID
    relationship_type: many_to_one
  $$
);
```

**Checkpoint:** Should return `Semantic view was successfully created.` Verify with:

```sql
SHOW SEMANTIC VIEWS IN SCHEMA AI_PLATFORM_DEV.SEMANTIC;
```

---

## 7. Phase 6: Cortex Agent

This creates the conversational agent that combines **Cortex Analyst** (text-to-SQL for analytics) and **Cortex Search** (semantic search over PODs).

```sql
CREATE OR REPLACE AGENT AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_AGENT
FROM SPECIFICATION $$
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 900,
      "tokens": 400000
    }
  },
  "instructions": {
    "orchestration": "You are LogisticsOps Assistant, a specialized logistics analytics agent for operations teams.\n\nYour Scope: Answer questions about shipments, orders, POD (Proof of Delivery) documents, claims, carriers, and customers.\n\nTool Selection Guidelines:\n- For KPI/analytics questions (on-time rates, costs, volumes, trends, comparisons): Use logistics_analytics.\n- For searching specific POD documents, exception details, damage descriptions: Use pod_search.\n- If a question needs both structured data AND document lookup, use both tools.\n\nBusiness Context:\n- POD = Proof of Delivery\n- Exception = any issue during delivery (damage, partial delivery, refused, missing signature)\n- On-time = delivered by the expected delivery date\n- Claims are filed by customers against carriers for damage, loss, shortage, or late delivery\n\nBoundaries:\n- You do NOT have real-time tracking data.\n- You cannot modify shipments, orders, or claims. Analytics and search only.\n- For production deployment architecture, tell the user to discuss with Shiva.",
    "response": "Be concise and professional. Lead with the direct answer, then supporting details. Use tables for multi-row results. Always include the time period and data scope. When showing rates, also show absolute numbers."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "logistics_analytics",
        "description": "Queries structured logistics data for KPI and analytics questions about shipments, orders, POD documents, claims, carriers, and customers. Key metrics: on_time_delivery_rate, exception_rate, damage_rate, missing_signature_count, total_freight_cost, total_claim_amount. Use for aggregated KPIs, trends, comparisons. Do NOT use for searching specific POD document text (use pod_search)."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "pod_search",
        "description": "Searches POD (Proof of Delivery) document content using semantic search. Finds specific POD records by text content, exception notes, damage descriptions, and metadata (POD_ID, SHIPMENT_ID, CUSTOMER_NAME, CARRIER_NAME, POD_STATUS). Use for finding specific PODs, exception details, damage descriptions. Do NOT use for aggregated analytics (use logistics_analytics)."
      }
    }
  ],
  "tool_resources": {
    "logistics_analytics": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "AI_WH"
      },
      "semantic_view": "AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_ANALYTICS"
    },
    "pod_search": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "AI_WH"
      },
      "search_service": "AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_SERVICE"
    }
  }
}
$$;
```

**Checkpoint:** Should return `Agent LOGISTICS_OPS_AGENT successfully created.` Verify with:

```sql
SHOW AGENTS LIKE 'LOGISTICS_OPS_AGENT' IN SCHEMA AI_PLATFORM_DEV.SEMANTIC;
DESCRIBE AGENT AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_AGENT;
```

---

## 8. Phase 7: Testing & Validation

### 8.1 Test the Semantic View (Cortex Analyst)

Go to **Snowsight > AI & ML > Cortex Analyst** or use the REST API to test these questions against the semantic view:

- "What is the on-time delivery rate by carrier?"
- "Show me POD exception rate by customer"
- "How many PODs have missing signatures?"
- "What are the total claims by carrier and type?"
- "Monthly shipment volume and freight cost trend"

### 8.2 Test the Cortex Search Service

```sql
-- Test semantic search
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_SERVICE',
    '{
      "query": "damaged deliveries for Acme Corp",
      "columns": ["POD_ID", "SHIPMENT_ID", "CUSTOMER_NAME", "POD_STATUS", "EXCEPTION_NOTES"],
      "limit": 5
    }'
  )
);
```

### 8.3 Test the Agent

Option A: **Snowflake Intelligence** (Snowsight > AI & ML > Intelligence) - register the agent and chat with it.

Option B: **REST API** - call the agent programmatically:

```sql
-- Quick test via SQL (if supported in your version)
-- Otherwise use Snowflake Intelligence UI or the REST API
```

### Sample Test Questions for the Agent

| Question | Expected Tool | What to Check |
|---|---|---|
| "What is our on-time delivery rate?" | logistics_analytics | Returns percentage + absolute numbers |
| "Show me damaged PODs for Acme Corp" | pod_search | Returns specific POD documents with damage details |
| "Top 5 carriers by freight cost" | logistics_analytics | Returns ranked table |
| "Find PODs with missing signatures in Mumbai" | pod_search | Returns PODs filtered by city + missing signature |
| "What is the claim resolution rate?" | logistics_analytics | Returns percentage |
| "Show me the POD for shipment SHP00123" | pod_search | Returns specific POD document |

---

## 9. Production Promotion Checklist

Before moving from DEV to production:

- [ ] **Data:** Replace sample data with actual production tables (point base_table references to your real schema)
- [ ] **POD Stage:** Switch from internal stage to external S3/ADLS/GCS stage
- [ ] **Role:** Create `AI_APP_ROLE` with least-privilege grants instead of ACCOUNTADMIN
- [ ] **Warehouse:** Size up `AI_WH` based on actual query volume and document processing load
- [ ] **Search refresh:** Consider using a DYNAMIC TABLE or VIEW for the search corpus instead of a static table
- [ ] **Semantic view:** Add verified queries (VQRs) based on actual user question patterns to improve accuracy
- [ ] **Agent instructions:** Tune tool descriptions and orchestration instructions based on testing feedback
- [ ] **Extraction quality:** Run AI_PARSE_DOCUMENT on a sample of real POD PDFs and compare extracted fields vs ground truth
- [ ] **Access control:** Grant USAGE on agent, semantic view, and search service to appropriate roles
- [ ] **Monitoring:** Set up task monitoring for the ingestion pipeline (TASK_HISTORY, error alerts)

---

## 10. Architecture Notes for Use Case 2

**Use Case 2 (Call Center Agent for Real-Time Conversation)** requires a separate architectural discussion with **Shiva** covering:

- Real-time vs near-real-time data requirements
- Channel integration (phone, Teams, web chat)
- User identity mapping to Snowflake roles
- Latency SLA (sub-second for live conversation?)
- Multi-agent patterns (routing, handoff, escalation)
- External tool integration (CRM, ticketing systems)
- Production tenancy model

**Do not implement Use Case 2 without this architectural review.**

---

## Quick Reference: Key Objects

| Object | Fully Qualified Name |
|---|---|
| Database | `AI_PLATFORM_DEV` |
| Warehouse | `AI_WH` |
| POD Stage | `AI_PLATFORM_DEV.RAW.POD_STAGE` |
| Extraction Task 1 | `AI_PLATFORM_DEV.RAW.POD_EXTRACTION_TASK` |
| Extraction Task 2 | `AI_PLATFORM_DEV.RAW.POD_FIELD_EXTRACTION_TASK` |
| Search Service | `AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_SERVICE` |
| Semantic View | `AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_ANALYTICS` |
| Agent | `AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_AGENT` |
