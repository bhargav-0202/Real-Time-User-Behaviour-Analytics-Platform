-- ============================================================
-- STEP 1: Use admin role and create warehouse
-- ============================================================
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60       -- Suspend after 60 seconds idle (saves credits)
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- ============================================================
-- STEP 2: Create the three databases
-- ============================================================
CREATE DATABASE IF NOT EXISTS RAW_DB;
CREATE DATABASE IF NOT EXISTS STAGING_DB;
CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;

-- ============================================================
-- STEP 3: Create schemas in each database
-- ============================================================
USE DATABASE RAW_DB;
CREATE SCHEMA IF NOT EXISTS EVENTS;

USE DATABASE STAGING_DB;
CREATE SCHEMA IF NOT EXISTS STG;

USE DATABASE ANALYTICS_DB;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- ============================================================
-- STEP 4: Create RAW tables (schema mirrors Kafka events exactly)
-- ============================================================
USE DATABASE RAW_DB;
USE SCHEMA EVENTS;
USE WAREHOUSE ANALYTICS_WH;

CREATE TABLE IF NOT EXISTS RAW_CLICKS (
    EVENT_ID          VARCHAR(36)     NOT NULL,
    EVENT_TYPE        VARCHAR(20),
    USER_ID           VARCHAR(36),
    SESSION_ID        VARCHAR(36),
    PRODUCT_ID        VARCHAR(20),
    PRODUCT_NAME      VARCHAR(255),
    PRODUCT_CATEGORY  VARCHAR(100),
    PAGE              VARCHAR(100),
    DEVICE            VARCHAR(50),
    COUNTRY           VARCHAR(10),
    REFERRER          VARCHAR(100),
    PRICE_SEEN        FLOAT,
    EVENT_TIMESTAMP   TIMESTAMP_TZ,
    INGESTED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_SESSIONS (
    EVENT_ID          VARCHAR(36)     NOT NULL,
    SESSION_ID        VARCHAR(36),
    USER_ID           VARCHAR(36),
    SESSION_START     TIMESTAMP_TZ,
    DURATION_SECONDS  INTEGER,
    PAGES_VISITED     INTEGER,
    DEVICE            VARCHAR(50),
    COUNTRY           VARCHAR(10),
    IS_NEW_USER       BOOLEAN,
    EVENT_TIMESTAMP   TIMESTAMP_TZ,
    INGESTED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_PURCHASES (
    EVENT_ID          VARCHAR(36)     NOT NULL,
    ORDER_ID          VARCHAR(36),
    USER_ID           VARCHAR(36),
    SESSION_ID        VARCHAR(36),
    PRODUCT_ID        VARCHAR(20),
    PRODUCT_NAME      VARCHAR(255),
    PRODUCT_CATEGORY  VARCHAR(100),
    QUANTITY          INTEGER,
    UNIT_PRICE        FLOAT,
    TOTAL_AMOUNT      FLOAT,
    PAYMENT_METHOD    VARCHAR(50),
    COUNTRY           VARCHAR(10),
    IS_FRAUDULENT     BOOLEAN,
    EVENT_TIMESTAMP   TIMESTAMP_TZ,
    INGESTED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- STEP 5: Create Snowflake Streams (CDC / Change Data Capture)
-- Streams track which rows are new since last consumption
-- ============================================================
CREATE STREAM IF NOT EXISTS RAW_CLICKS_STREAM ON TABLE RAW_CLICKS;
CREATE STREAM IF NOT EXISTS RAW_SESSIONS_STREAM ON TABLE RAW_SESSIONS;
CREATE STREAM IF NOT EXISTS RAW_PURCHASES_STREAM ON TABLE RAW_PURCHASES;

-- ============================================================
-- STEP 6: Create a dbt service user with appropriate permissions
-- ============================================================
CREATE ROLE IF NOT EXISTS DBT_ROLE;
CREATE USER IF NOT EXISTS DBT_USER
    PASSWORD = 'YourStrongPassword123!'
    DEFAULT_ROLE = DBT_ROLE
    DEFAULT_WAREHOUSE = ANALYTICS_WH;

GRANT ROLE DBT_ROLE TO USER DBT_USER;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE DBT_ROLE;

-- Grant read access to RAW_DB
GRANT USAGE ON DATABASE RAW_DB TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA RAW_DB.EVENTS TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DB.EVENTS TO ROLE DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_DB.EVENTS TO ROLE DBT_ROLE;

-- Grant full access to STAGING_DB and ANALYTICS_DB (dbt creates tables here)
GRANT ALL PRIVILEGES ON DATABASE STAGING_DB TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON DATABASE ANALYTICS_DB TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE STAGING_DB TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ANALYTICS_DB TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON ALL FUTURE SCHEMAS IN DATABASE STAGING_DB TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON ALL FUTURE SCHEMAS IN DATABASE ANALYTICS_DB TO ROLE DBT_ROLE;


