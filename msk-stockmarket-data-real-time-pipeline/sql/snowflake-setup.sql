-- ============================================================
-- Snowflake Setup — Run each block in order in a SQL Worksheet
-- Replace <ACCOUNT-ID> with your 12-digit AWS account ID
-- ============================================================

-- Step 1: Database & Table
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE KAFKA_DB;
USE DATABASE KAFKA_DB;
CREATE SCHEMA KAFKA_SCHEMA;
USE SCHEMA KAFKA_SCHEMA;

CREATE OR REPLACE TABLE STOCK_MARKET_DATA (
    symbol      VARCHAR(10),
    price       FLOAT,
    volume      INTEGER,
    change      FLOAT,
    timestamp   VARCHAR(30),
    exchange    VARCHAR(20),
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

DESCRIBE TABLE STOCK_MARKET_DATA;


-- Step 2: Storage Integration
-- ─────────────────────────────────────────────────────────────
-- Run this first, then use DESC to get the IAM user ARN & external ID
-- needed to create the snowflake-s3-role in AWS IAM.

CREATE OR REPLACE STORAGE INTEGRATION s3_kafka_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT-ID>:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://kafka-msk-snowflake-bucket/topics/stock-market-data/');

DESC INTEGRATION s3_kafka_integration;
-- Copy from output:
--   STORAGE_AWS_IAM_USER_ARN  →  used as Principal in trust policy
--   STORAGE_AWS_EXTERNAL_ID   →  used as sts:ExternalId condition


-- Step 3: External Stage
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE STAGE kafka_s3_stage
    URL = 's3://kafka-msk-snowflake-bucket/topics/stock-market-data/'
    STORAGE_INTEGRATION = s3_kafka_integration
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE);

LIST @kafka_s3_stage;


-- Step 4: Manual test load (run once to verify stage is working)
-- ─────────────────────────────────────────────────────────────
COPY INTO STOCK_MARKET_DATA (symbol, price, volume, change, timestamp, exchange)
FROM (
    SELECT
        $1:symbol::VARCHAR,
        $1:price::FLOAT,
        $1:volume::INTEGER,
        $1:change::FLOAT,
        $1:timestamp::VARCHAR,
        $1:exchange::VARCHAR
    FROM @kafka_s3_stage
)
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

SELECT * FROM STOCK_MARKET_DATA LIMIT 10;


-- Step 5: Snowpipe (auto-ingest)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PIPE kafka_snowpipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO STOCK_MARKET_DATA (symbol, price, volume, change, timestamp, exchange)
    FROM (
        SELECT
            $1:symbol::VARCHAR,
            $1:price::FLOAT,
            $1:volume::INTEGER,
            $1:change::FLOAT,
            $1:timestamp::VARCHAR,
            $1:exchange::VARCHAR
        FROM @kafka_s3_stage
    )
    FILE_FORMAT = (TYPE = 'JSON');

SHOW PIPES;
-- Copy the notification_channel ARN — paste into S3 event notification (Step 8.6 in README)


-- Step 6: Validation queries
-- ─────────────────────────────────────────────────────────────
SELECT SYSTEM$PIPE_STATUS('kafka_snowpipe');

SELECT COUNT(*) FROM STOCK_MARKET_DATA;

SELECT
    SYMBOL,
    COUNT(*)      AS RECORDS,
    AVG(PRICE)    AS AVG_PRICE,
    MIN(PRICE)    AS MIN_PRICE,
    MAX(PRICE)    AS MAX_PRICE
FROM STOCK_MARKET_DATA
GROUP BY SYMBOL
ORDER BY SYMBOL;
