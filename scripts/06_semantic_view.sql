/*
 * Script 06: Semantic View for Cortex Analyst
 * Creates: LOGISTICS_OPS_ANALYTICS semantic view via SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML
 * 6 tables, 7 relationships, KPI metrics for logistics operations
 */

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
        sample_values: [Mumbai, Chennai, Delhi, Bangalore, Pune]
      - name: DESTINATION_CITY
        expr: DESTINATION_CITY
        data_type: TEXT
        description: City where the shipment is being delivered
        sample_values: [Mumbai, Chennai, Delhi, Hyderabad, Kolkata]
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
