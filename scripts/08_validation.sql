/*
 * Script 08: Validation & Testing
 * Run after scripts 01-07 to verify everything is working
 */

--------------------------------------------------------------------
-- 8.1 Verify all objects exist
--------------------------------------------------------------------
SHOW SCHEMAS IN DATABASE AI_PLATFORM_DEV;
SHOW STAGES IN SCHEMA AI_PLATFORM_DEV.RAW;
SHOW TASKS IN SCHEMA AI_PLATFORM_DEV.RAW;
SHOW CORTEX SEARCH SERVICES IN SCHEMA AI_PLATFORM_DEV.SEMANTIC;
SHOW SEMANTIC VIEWS IN SCHEMA AI_PLATFORM_DEV.SEMANTIC;
SHOW AGENTS LIKE 'LOGISTICS_OPS_AGENT' IN SCHEMA AI_PLATFORM_DEV.SEMANTIC;

--------------------------------------------------------------------
-- 8.2 Verify table row counts
--------------------------------------------------------------------
SELECT 'FACT_SHIPMENTS' AS TBL, COUNT(*) AS CNT FROM AI_PLATFORM_DEV.CURATED.FACT_SHIPMENTS
UNION ALL SELECT 'FACT_ORDERS', COUNT(*) FROM AI_PLATFORM_DEV.CURATED.FACT_ORDERS
UNION ALL SELECT 'POD_FACT', COUNT(*) FROM AI_PLATFORM_DEV.CURATED.POD_FACT
UNION ALL SELECT 'FACT_CLAIMS', COUNT(*) FROM AI_PLATFORM_DEV.CURATED.FACT_CLAIMS
UNION ALL SELECT 'DIM_CUSTOMERS', COUNT(*) FROM AI_PLATFORM_DEV.CURATED.DIM_CUSTOMERS
UNION ALL SELECT 'DIM_CARRIERS', COUNT(*) FROM AI_PLATFORM_DEV.CURATED.DIM_CARRIERS
UNION ALL SELECT 'POD_SEARCH_CORPUS', COUNT(*) FROM AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_CORPUS;

--------------------------------------------------------------------
-- 8.3 Verify Cortex Search is ACTIVE
--------------------------------------------------------------------
SELECT NAME, INDEXING_STATE, SERVING_STATE, SOURCE_DATA_NUM_ROWS
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-5)));
-- Expect: INDEXING_STATE = ACTIVE, SERVING_STATE = ACTIVE

--------------------------------------------------------------------
-- 8.4 Test Cortex Search directly
--------------------------------------------------------------------
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_SERVICE',
    '{
      "query": "damaged deliveries",
      "columns": ["POD_ID", "SHIPMENT_ID", "CUSTOMER_NAME", "POD_STATUS", "EXCEPTION_NOTES"],
      "limit": 5
    }'
  )
);

--------------------------------------------------------------------
-- 8.5 Test Semantic View with sample queries
--------------------------------------------------------------------
-- These should work once the semantic view is active:
-- Go to Snowsight > AI & ML > Cortex Analyst
-- Select AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_ANALYTICS
-- Ask: "What is the on-time delivery rate by carrier?"
-- Ask: "Show me POD exception rate by customer"
-- Ask: "Total claims by type"

--------------------------------------------------------------------
-- 8.6 Test Agent
--------------------------------------------------------------------
-- Option A: Snowflake Intelligence (Snowsight > AI & ML > Intelligence)
-- Option B: Cortex Agent REST API
-- Sample questions to test:
--   "What is our on-time delivery rate?"
--   "Show me damaged PODs for Acme Corp"
--   "Top 5 carriers by freight cost"
--   "Find PODs with missing signatures in Mumbai"
--   "What is the claim resolution rate?"
