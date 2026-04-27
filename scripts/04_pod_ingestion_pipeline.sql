/*
 * Script 04: POD Document Ingestion Pipeline
 * Creates: RAW tables, streams, and tasks for AI_PARSE_DOCUMENT + AI_EXTRACT
 * Tasks are created SUSPENDED - resume when POD files are ready
 *
 * Cost: AI_PARSE_DOCUMENT LAYOUT = ~3.33 credits/1000 pages
 *       AI_PARSE_DOCUMENT OCR    = ~0.5 credits/1000 pages
 */

-- Raw file registry
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

-- Parsed document output
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

-- Task 1: Parse PDFs with AI_PARSE_DOCUMENT (LAYOUT mode)
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

-- Task 2: Extract structured fields with AI_EXTRACT (child task)
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

/*
 * To activate the pipeline, run (resume child FIRST, then parent):
 *
 *   ALTER TASK AI_PLATFORM_DEV.RAW.POD_FIELD_EXTRACTION_TASK RESUME;
 *   ALTER TASK AI_PLATFORM_DEV.RAW.POD_EXTRACTION_TASK RESUME;
 *
 * To test: upload a sample POD PDF:
 *   PUT file:///path/to/sample_pod.pdf @AI_PLATFORM_DEV.RAW.POD_STAGE;
 *   ALTER STAGE AI_PLATFORM_DEV.RAW.POD_STAGE REFRESH;
 *   INSERT INTO AI_PLATFORM_DEV.RAW.POD_FILES (FILE_NAME, FILE_RELATIVE_PATH, FILE_TYPE)
 *     SELECT RELATIVE_PATH, RELATIVE_PATH, 'PDF'
 *     FROM DIRECTORY(@AI_PLATFORM_DEV.RAW.POD_STAGE);
 */
