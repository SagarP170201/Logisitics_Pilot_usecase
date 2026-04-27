/*
 * Script 02: Dimension Tables
 * Creates: DIM_CARRIERS, DIM_CUSTOMERS with sample data
 * >>> Replace sample INSERT statements with your actual data <<<
 */

CREATE OR REPLACE TABLE AI_PLATFORM_DEV.CURATED.DIM_CARRIERS (
    CARRIER_ID VARCHAR(20) PRIMARY KEY,
    CARRIER_NAME VARCHAR(200),
    CARRIER_TYPE VARCHAR(50),
    SERVICE_REGION VARCHAR(100),
    ON_TIME_RATING FLOAT,
    ACTIVE_FLAG BOOLEAN DEFAULT TRUE
);

INSERT INTO AI_PLATFORM_DEV.CURATED.DIM_CARRIERS VALUES
('CAR001', 'BlueDart Express', 'Express', 'Pan-India', 0.92, TRUE),
('CAR002', 'Delhivery', 'Standard', 'Pan-India', 0.87, TRUE),
('CAR003', 'DTDC Courier', 'Economy', 'Pan-India', 0.81, TRUE),
('CAR004', 'Gati Ltd', 'Freight', 'North & West India', 0.85, TRUE),
('CAR005', 'Rivigo', 'Long-Haul', 'Pan-India', 0.89, TRUE),
('CAR006', 'Ecom Express', 'E-commerce', 'Metro Cities', 0.90, TRUE),
('CAR007', 'Shadowfax', 'Last-Mile', 'Metro Cities', 0.88, TRUE),
('CAR008', 'XpressBees', 'Express', 'Pan-India', 0.86, TRUE);

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

INSERT INTO AI_PLATFORM_DEV.CURATED.DIM_CUSTOMERS VALUES
('CUST001', 'Acme Corp', 'Enterprise', 'Mumbai', 'Maharashtra', 'India', 'logistics@acme.com', '2024-01-15'),
('CUST002', 'Beta Industries', 'Mid-Market', 'Chennai', 'Tamil Nadu', 'India', 'ops@beta.in', '2024-02-10'),
('CUST003', 'Gamma Retail', 'SMB', 'Delhi', 'Delhi', 'India', 'supply@gamma.in', '2024-03-01'),
('CUST004', 'Delta Pharma', 'Enterprise', 'Hyderabad', 'Telangana', 'India', 'dist@deltapharma.com', '2024-04-20'),
('CUST005', 'Epsilon Electronics', 'Mid-Market', 'Bangalore', 'Karnataka', 'India', 'fulfillment@epsilon.in', '2024-05-12'),
('CUST006', 'Zeta Foods', 'SMB', 'Pune', 'Maharashtra', 'India', 'delivery@zetafoods.in', '2024-06-01'),
('CUST007', 'Eta Auto Parts', 'Enterprise', 'Ahmedabad', 'Gujarat', 'India', 'logistics@etaauto.com', '2024-07-15'),
('CUST008', 'Theta Textiles', 'Mid-Market', 'Kolkata', 'West Bengal', 'India', 'supply@theta.in', '2024-08-22'),
('CUST009', 'Iota Chemicals', 'Enterprise', 'Jaipur', 'Rajasthan', 'India', 'ops@iota.com', '2024-09-10'),
('CUST010', 'Kappa Logistics', 'SMB', 'Lucknow', 'Uttar Pradesh', 'India', 'dispatch@kappa.in', '2024-10-05');
