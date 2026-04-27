/*
 * Script 03: Fact Tables with Sample Data
 * Creates: FACT_SHIPMENTS, FACT_ORDERS, POD_FACT, FACT_CLAIMS
 * >>> Replace sample data generators with your actual data loads <<<
 */

-- Shipments
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

INSERT INTO AI_PLATFORM_DEV.CURATED.FACT_SHIPMENTS
SELECT * FROM (
  SELECT
    'SHP' || LPAD(SEQ4()::VARCHAR, 5, '0') AS SHIPMENT_ID,
    'ORD' || LPAD(SEQ4()::VARCHAR, 5, '0') AS ORDER_ID,
    'CUST' || LPAD(UNIFORM(1, 10, RANDOM())::VARCHAR, 3, '0') AS CUSTOMER_ID,
    'CAR' || LPAD(UNIFORM(1, 8, RANDOM())::VARCHAR, 3, '0') AS CARRIER_ID,
    ARRAY_CONSTRUCT('Mumbai','Chennai','Delhi','Hyderabad','Bangalore','Pune','Ahmedabad','Kolkata')[UNIFORM(0,7,RANDOM())]::VARCHAR AS ORIGIN_CITY,
    ARRAY_CONSTRUCT('Mumbai','Chennai','Delhi','Hyderabad','Bangalore','Pune','Ahmedabad','Kolkata','Jaipur','Lucknow')[UNIFORM(0,9,RANDOM())]::VARCHAR AS DESTINATION_CITY,
    DATEADD('day', -UNIFORM(1, 180, RANDOM()), CURRENT_DATE()) AS SHIP_DATE,
    DATEADD('day', -UNIFORM(1, 180, RANDOM()) + UNIFORM(2, 7, RANDOM()), CURRENT_DATE()) AS EXPECTED_DELIVERY_DATE,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 85
         THEN DATEADD('day', -UNIFORM(1, 175, RANDOM()), CURRENT_DATE())
         ELSE NULL END AS ACTUAL_DELIVERY_DATE,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 70 THEN 'Delivered'
         WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN 'In Transit'
         WHEN UNIFORM(1,100,RANDOM()) <= 92 THEN 'Out for Delivery'
         WHEN UNIFORM(1,100,RANDOM()) <= 96 THEN 'Exception'
         ELSE 'Returned' END AS STATUS,
    ROUND(UNIFORM(1, 500, RANDOM()) + RANDOM() / 1e18, 2) AS WEIGHT_KG,
    UNIFORM(1, 20, RANDOM()) AS NUM_PACKAGES,
    ROUND(UNIFORM(50, 5000, RANDOM()) + RANDOM() / 1e18, 2) AS FREIGHT_COST,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 82 THEN TRUE ELSE FALSE END AS IS_ON_TIME,
    CURRENT_TIMESTAMP() AS CREATED_AT
  FROM TABLE(GENERATOR(ROWCOUNT => 500))
);

-- Orders
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

INSERT INTO AI_PLATFORM_DEV.CURATED.FACT_ORDERS
SELECT * FROM (
  SELECT
    'ORD' || LPAD(SEQ4()::VARCHAR, 5, '0') AS ORDER_ID,
    'CUST' || LPAD(UNIFORM(1, 10, RANDOM())::VARCHAR, 3, '0') AS CUSTOMER_ID,
    DATEADD('day', -UNIFORM(1, 180, RANDOM()), CURRENT_DATE()) AS ORDER_DATE,
    ROUND(UNIFORM(500, 50000, RANDOM()) + RANDOM() / 1e18, 2) AS ORDER_VALUE,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 75 THEN 'Fulfilled'
         WHEN UNIFORM(1,100,RANDOM()) <= 90 THEN 'Processing'
         WHEN UNIFORM(1,100,RANDOM()) <= 95 THEN 'Cancelled'
         ELSE 'Returned' END AS ORDER_STATUS,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 40 THEN 'Credit Card'
         WHEN UNIFORM(1,100,RANDOM()) <= 70 THEN 'UPI'
         WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN 'Net Banking'
         ELSE 'COD' END AS PAYMENT_METHOD,
    UNIFORM(1, 50, RANDOM()) AS NUM_ITEMS,
    CURRENT_TIMESTAMP() AS CREATED_AT
  FROM TABLE(GENERATOR(ROWCOUNT => 500))
);

-- POD Fact
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

INSERT INTO AI_PLATFORM_DEV.CURATED.POD_FACT
SELECT * FROM (
  SELECT
    'POD' || LPAD(SEQ4()::VARCHAR, 5, '0') AS POD_ID,
    'SHP' || LPAD(UNIFORM(0, 499, RANDOM())::VARCHAR, 5, '0') AS SHIPMENT_ID,
    'ORD' || LPAD(UNIFORM(0, 499, RANDOM())::VARCHAR, 5, '0') AS ORDER_ID,
    'CUST' || LPAD(UNIFORM(1, 10, RANDOM())::VARCHAR, 3, '0') AS CUSTOMER_ID,
    'CAR' || LPAD(UNIFORM(1, 8, RANDOM())::VARCHAR, 3, '0') AS CARRIER_ID,
    DATEADD('day', -UNIFORM(1, 160, RANDOM()), CURRENT_DATE()) AS DELIVERY_DATE,
    ARRAY_CONSTRUCT('Rajesh Kumar','Priya Sharma','Amit Patel','Sita Reddy','Vikram Singh','Meena Devi','Arjun Nair','Deepa Joshi','Rahul Gupta','Anita Rao')[UNIFORM(0,9,RANDOM())]::VARCHAR AS SIGNED_BY,
    ARRAY_CONSTRUCT('Warehouse Manager','Security Guard','Receptionist','Store Keeper','Operations Head','Logistics Coord','Shipping Clerk','Floor Supervisor')[UNIFORM(0,7,RANDOM())]::VARCHAR AS RECEIVER_NAME,
    ARRAY_CONSTRUCT('Plot 45, MIDC Industrial Area','Survey No 120, IT Park Road','Block B, Sector 62','Unit 7, SEZ Phase 2','Warehouse 3, Logistics Hub','Building 12, Tech Park','Godown 5, Transport Nagar','Factory Unit 9, GIDC')[UNIFORM(0,7,RANDOM())]::VARCHAR AS DELIVERY_ADDRESS,
    ARRAY_CONSTRUCT('Mumbai','Chennai','Delhi','Hyderabad','Bangalore','Pune','Ahmedabad','Kolkata','Jaipur','Lucknow')[UNIFORM(0,9,RANDOM())]::VARCHAR AS DELIVERY_CITY,
    ARRAY_CONSTRUCT('Maharashtra','Tamil Nadu','Delhi','Telangana','Karnataka','Maharashtra','Gujarat','West Bengal','Rajasthan','Uttar Pradesh')[UNIFORM(0,9,RANDOM())]::VARCHAR AS DELIVERY_STATE,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 75 THEN 'Delivered - Clean'
         WHEN UNIFORM(1,100,RANDOM()) <= 88 THEN 'Delivered - With Exceptions'
         WHEN UNIFORM(1,100,RANDOM()) <= 94 THEN 'Partial Delivery'
         WHEN UNIFORM(1,100,RANDOM()) <= 97 THEN 'Refused'
         ELSE 'Undeliverable' END AS POD_STATUS,
    CASE WHEN UNIFORM(1,100,RANDOM()) > 75 THEN TRUE ELSE FALSE END AS EXCEPTION_FLAG,
    CASE WHEN UNIFORM(1,100,RANDOM()) > 75 THEN
      ARRAY_CONSTRUCT(
        'Package found with torn outer packaging. Contents appear intact.',
        'Delivery attempted but receiver not available. Left with security guard.',
        'One package out of shipment missing. Partial delivery accepted.',
        'Water damage observed on 2 cartons. Customer accepted undamaged units.',
        'Wrong delivery address on label. Rerouted with 2 day delay.',
        'Customer refused delivery citing wrong items received.',
        'Signature mismatch with authorized receiver list.',
        'Late delivery beyond SLA window. Credit adjustment flagged.',
        'Temperature-sensitive shipment delivered outside specified range.',
        'Multiple delivery attempts required. Final delivery on 3rd attempt.'
      )[UNIFORM(0,9,RANDOM())]::VARCHAR
    ELSE NULL END AS EXCEPTION_NOTES,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 92 THEN TRUE ELSE FALSE END AS SIGNATURE_PRESENT,
    CASE WHEN UNIFORM(1,100,RANDOM()) > 88 THEN TRUE ELSE FALSE END AS DAMAGE_REPORTED,
    CASE WHEN UNIFORM(1,100,RANDOM()) > 88 THEN
      ARRAY_CONSTRUCT('Outer packaging torn','Water damage on cartons','Crushed box','Dented container','Broken seal on pallet')[UNIFORM(0,4,RANDOM())]::VARCHAR
    ELSE NULL END AS DAMAGE_DESCRIPTION,
    CASE WHEN UNIFORM(1,100,RANDOM()) > 90 THEN TRUE ELSE FALSE END AS PARTIAL_DELIVERY,
    UNIFORM(1, 20, RANDOM()) AS PACKAGES_RECEIVED,
    UNIFORM(1, 20, RANDOM()) AS PACKAGES_EXPECTED,
    'POD_' || LPAD(SEQ4()::VARCHAR, 5, '0') || '.pdf' AS POD_FILE_NAME,
    'PROOF OF DELIVERY' || CHR(10) || '---' || CHR(10) ||
    'POD Reference: POD' || LPAD(SEQ4()::VARCHAR, 5, '0') || CHR(10) ||
    'Shipment ID: SHP' || LPAD(UNIFORM(0, 499, RANDOM())::VARCHAR, 5, '0') || CHR(10) ||
    'Delivery Date: ' || DATEADD('day', -UNIFORM(1, 160, RANDOM()), CURRENT_DATE())::VARCHAR || CHR(10) ||
    'Carrier: ' || ARRAY_CONSTRUCT('BlueDart Express','Delhivery','DTDC Courier','Gati Ltd','Rivigo','Ecom Express','Shadowfax','XpressBees')[UNIFORM(0,7,RANDOM())]::VARCHAR || CHR(10) ||
    'Delivered To: ' || ARRAY_CONSTRUCT('Acme Corp','Beta Industries','Gamma Retail','Delta Pharma','Epsilon Electronics')[UNIFORM(0,4,RANDOM())]::VARCHAR || CHR(10) ||
    'Signed By: ' || ARRAY_CONSTRUCT('Rajesh Kumar','Priya Sharma','Amit Patel','Sita Reddy','Vikram Singh')[UNIFORM(0,4,RANDOM())]::VARCHAR || CHR(10) ||
    'Packages: ' || UNIFORM(1,20,RANDOM())::VARCHAR || ' received' || CHR(10) ||
    'Status: ' || ARRAY_CONSTRUCT('Delivered - Clean','Delivered - With Exceptions','Partial Delivery','Refused')[UNIFORM(0,3,RANDOM())]::VARCHAR
    AS POD_TEXT_CONTENT,
    ROUND(UNIFORM(75, 99, RANDOM()) + RANDOM() / 1e18, 2) / 100.0 AS EXTRACTION_CONFIDENCE,
    CURRENT_TIMESTAMP() AS PROCESSED_AT
  FROM TABLE(GENERATOR(ROWCOUNT => 400))
);

-- Claims
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

INSERT INTO AI_PLATFORM_DEV.CURATED.FACT_CLAIMS
SELECT * FROM (
  SELECT
    'CLM' || LPAD(SEQ4()::VARCHAR, 5, '0') AS CLAIM_ID,
    'SHP' || LPAD(UNIFORM(0, 499, RANDOM())::VARCHAR, 5, '0') AS SHIPMENT_ID,
    'POD' || LPAD(UNIFORM(0, 399, RANDOM())::VARCHAR, 5, '0') AS POD_ID,
    'CUST' || LPAD(UNIFORM(1, 10, RANDOM())::VARCHAR, 3, '0') AS CUSTOMER_ID,
    'CAR' || LPAD(UNIFORM(1, 8, RANDOM())::VARCHAR, 3, '0') AS CARRIER_ID,
    DATEADD('day', -UNIFORM(1, 150, RANDOM()), CURRENT_DATE()) AS CLAIM_DATE,
    ARRAY_CONSTRUCT('Damage','Loss','Shortage','Late Delivery','Wrong Delivery','Missing Signature')[UNIFORM(0,5,RANDOM())]::VARCHAR AS CLAIM_TYPE,
    ROUND(UNIFORM(500, 25000, RANDOM()) + RANDOM() / 1e18, 2) AS CLAIM_AMOUNT,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 40 THEN 'Resolved'
         WHEN UNIFORM(1,100,RANDOM()) <= 70 THEN 'Under Review'
         WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN 'Pending Documentation'
         ELSE 'Rejected' END AS CLAIM_STATUS,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 40
         THEN DATEADD('day', -UNIFORM(1, 120, RANDOM()), CURRENT_DATE())
         ELSE NULL END AS RESOLUTION_DATE,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 40
         THEN ROUND(UNIFORM(200, 20000, RANDOM()) + RANDOM() / 1e18, 2)
         ELSE NULL END AS RESOLUTION_AMOUNT,
    ARRAY_CONSTRUCT(
      'Shipment arrived with visible damage. Contents partially damaged.',
      'Package missing from multi-piece shipment.',
      'Delivery exceeded SLA by more than 48 hours.',
      'Items delivered to wrong address.',
      'POD shows signature but customer claims non-receipt.',
      'Temperature-sensitive goods delivered outside range.',
      'Carrier lost shipment in transit. No tracking for 7+ days.',
      'Customer rejected delivery due to damaged packaging.'
    )[UNIFORM(0,7,RANDOM())]::VARCHAR AS DESCRIPTION,
    CURRENT_TIMESTAMP() AS CREATED_AT
  FROM TABLE(GENERATOR(ROWCOUNT => 80))
);
