# Ecommerce Airflow Data Pipeline

## Overview

This project implements an end-to-end batch data pipeline for ecommerce data using Apache Airflow for orchestration, AWS S3 for data lake storage, and Snowflake for data warehousing. The pipeline processes customer and order data through multiple stages with dynamic batch processing.

## Project Objective

Build a production-grade data pipeline that:
- Orchestrates batch data processing workflows
- Implements a three-tier data lake architecture (landing, processing, processed)
- Loads data into Snowflake data warehouse
- Performs data transformations and aggregations
- Supports dynamic batch IDs for data lineage tracking

## Architecture

![Ecommerce Pipeline Architecture](./e%20commrece%20pipline.png)

```
┌─────────────────┐
│  Data Sources   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  S3 Landing     │ (Raw data ingestion)
└────────┬────────┘
         │ Airflow DAG
         ▼
┌─────────────────┐
│  S3 Processing  │ (In-flight data)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Snowflake     │ (Data warehouse)
│   COPY INTO     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  S3 Processed   │ (Archive)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transformations │ (Analytics-ready)
└─────────────────┘
```

## Technology Stack

- **Apache Airflow**: Workflow orchestration
- **AWS S3**: Data lake storage
- **Snowflake**: Cloud data warehouse
- **Python**: DAG development
- **AWS CLI**: S3 operations

## Project Structure

```
EcommerceAirflowDataPipeline/
├── README.md
├── dag.py                    # Main Airflow DAG
├── simple_3_task_dag.py      # Simple example DAG
└── ecommerce-airflow/        # Airflow configuration
```

## Pipeline Features

### 1. Three-Tier Data Lake Architecture

**Landing Zone**
- Raw data ingestion point
- Organized by date hierarchy: `YYYY/MM/DD/HH`
- Temporary storage before processing

**Processing Zone**
- In-flight data during pipeline execution
- Organized by batch ID for tracking
- Isolated from landing to prevent conflicts

**Processed Zone**
- Successfully processed data archive
- Maintains data lineage with batch IDs
- Used for reprocessing and auditing

### 2. Dynamic Batch Processing

- Supports manual batch date specification via `dag_run.conf`
- Falls back to logical execution date if not provided
- Unique batch IDs for data lineage tracking

### 3. Parallel Processing

The DAG processes customers and orders in parallel:
```
Start
  ├─> Customer Pipeline
  │   ├─> Landing → Processing
  │   ├─> Load to Snowflake
  │   └─> Processing → Processed
  │
  └─> Orders Pipeline
      ├─> Landing → Processing
      ├─> Load to Snowflake
      └─> Processing → Processed
          │
          └─> Join & Transform → Analytics Table
```

### 4. Snowflake Integration

- Uses Snowflake stages for efficient data loading
- COPY INTO commands for bulk loading
- Performs joins and aggregations in Snowflake
- Creates analytics-ready tables

## Installation & Setup

### Prerequisites

```bash
Python 3.7+
Apache Airflow 2.x
AWS CLI configured
Snowflake account
```

### Install Dependencies

```bash
pip install apache-airflow
pip install apache-airflow-providers-snowflake
pip install apache-airflow-providers-amazon
pip install boto3
```

### Airflow Configuration

1. Set up Airflow connections:

**Snowflake Connection**
```bash
airflow connections add 'snowflake_conn' \
    --conn-type 'snowflake' \
    --conn-login 'your_username' \
    --conn-password 'your_password' \
    --conn-schema 'RETAIL_SCHEMA' \
    --conn-extra '{
        "account": "your_account",
        "warehouse": "your_warehouse",
        "database": "RETAIL_DB",
        "region": "us-east-1"
    }'
```

**AWS Connection**
```bash
# Configure AWS CLI or set environment variables
aws configure
```

### Snowflake Setup

```sql
-- Create database and schema
CREATE DATABASE RETAIL_DB;
CREATE SCHEMA RETAIL_DB.RETAIL_SCHEMA;

-- Create raw tables
CREATE TABLE RETAIL_DB.RETAIL_SCHEMA.CUSTOMERS_RAW (
    C_BATCH_ID VARCHAR,
    C_CUSTKEY NUMBER,
    C_NAME VARCHAR,
    C_ADDRESS VARCHAR,
    C_NATIONKEY NUMBER,
    C_PHONE VARCHAR,
    C_ACCTBAL NUMBER,
    C_MKTSEGMENT VARCHAR,
    C_COMMENT VARCHAR
);

CREATE TABLE RETAIL_DB.RETAIL_SCHEMA.ORDERS_RAW (
    O_BATCH_ID VARCHAR,
    O_ORDERKEY NUMBER,
    O_CUSTKEY NUMBER,
    O_ORDERSTATUS VARCHAR,
    O_TOTALPRICE NUMBER,
    O_ORDERDATE DATE,
    O_ORDERPRIORITY VARCHAR,
    O_CLERK VARCHAR,
    O_SHIPPRIORITY NUMBER,
    O_COMMENT VARCHAR
);

-- Create analytics table
CREATE TABLE RETAIL_DB.RETAIL_SCHEMA.ORDER_CUSTOMER_DATE_PRICE (
    C_NAME VARCHAR,
    O_ORDERDATE DATE,
    TOTAL_PRICE NUMBER,
    C_BATCH_ID VARCHAR
);

-- Create external stages
CREATE STAGE CUSTOMER_RAW_STAGE
    URL = 's3://ecommerece-datapipeline-qh/firehouse/customers/processing/'
    CREDENTIALS = (AWS_KEY_ID='xxx' AWS_SECRET_KEY='xxx');

CREATE STAGE ORDERS_RAW_STAGE
    URL = 's3://ecommerece-datapipeline-qh/firehouse/orders/processing/'
    CREDENTIALS = (AWS_KEY_ID='xxx' AWS_SECRET_KEY='xxx');
```

### S3 Bucket Structure

```
s3://ecommerece-datapipeline-qh/
└── firehouse/
    ├── customers/
    │   ├── landing/YYYY/MM/DD/HH/
    │   ├── processing/BATCH_ID/
    │   └── processed/BATCH_ID/
    └── orders/
        ├── landing/YYYY/MM/DD/HH/
        ├── processing/BATCH_ID/
        └── processed/BATCH_ID/
```

## Usage

### Running the Pipeline

**Manual Trigger (Default)**
```bash
airflow dags trigger customer_orders_datapipeline_dynamic_batch_id
```

**With Custom Batch Date**
```bash
airflow dags trigger customer_orders_datapipeline_dynamic_batch_id \
    --conf '{"batch_date": "2024/04/22/14"}'
```

### Monitoring

```bash
# View DAG status
airflow dags list

# Check task status
airflow tasks list customer_orders_datapipeline_dynamic_batch_id

# View logs
airflow tasks logs customer_orders_datapipeline_dynamic_batch_id <task_id> <execution_date>
```

## Pipeline Tasks

### 1. Data Movement Tasks
- `customer_landing_to_processing`: Move customer data to processing zone
- `orders_landing_to_processing`: Move order data to processing zone
- `customer_processing_to_processed`: Archive processed customer data
- `orders_processing_to_processed`: Archive processed order data

### 2. Snowflake Load Tasks
- `snowflake_raw_insert_customers`: Load customers into Snowflake
- `snowflake_raw_insert_order`: Load orders into Snowflake

### 3. Transformation Tasks
- `snowflake_order_customers_small_transformation`: Join and aggregate data

## Data Transformations

The pipeline performs the following transformation:
```sql
-- Aggregate order totals by customer and date
-- Filter for completed orders (status = 'F')
SELECT 
    c.C_NAME,
    o.O_ORDERDATE,
    SUM(o.O_TOTALPRICE) as TOTAL_PRICE,
    c.C_BATCH_ID
FROM ORDERS_RAW o
JOIN CUSTOMERS_RAW c 
  ON o.O_CUSTKEY = c.C_CUSTKEY
 AND o.O_BATCH_ID = c.C_BATCH_ID
WHERE o.O_ORDERSTATUS = 'F'
GROUP BY C_NAME, O_ORDERDATE, c.C_BATCH_ID
```

## Key Features

- **Idempotency**: Pipeline can be safely re-run
- **Error Handling**: Graceful handling of missing data
- **Parallel Execution**: Independent customer and order processing
- **Data Lineage**: Batch ID tracking throughout pipeline
- **Scalability**: Handles large data volumes efficiently

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Airflow connection error | Verify Snowflake connection configuration |
| S3 access denied | Check AWS credentials and IAM permissions |
| No data for batch | Verify data exists in landing zone for specified date |
| Snowflake stage error | Ensure stages are created and credentials are valid |

## Future Enhancements

- Add data quality checks
- Implement incremental loading
- Add email notifications for failures
- Create data validation tasks
- Implement SLA monitoring
- Add dbt for complex transformations

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Snowflake COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html)
- [AWS S3 CLI](https://docs.aws.amazon.com/cli/latest/reference/s3/)
