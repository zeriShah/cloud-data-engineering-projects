from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# -------------------------
# CONNECTION IDS
# -------------------------
AWS_CONN_ID = "aws_s3_conn"
SNOWFLAKE_CONN_ID = "snowflake_conn"

# -------------------------
# DEFAULT CONFIG
# -------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="s3_raw_to_snowflake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["s3", "snowflake", "etl"],
) as dag:

    # -------------------------
    # DYNAMIC BATCH
    # -------------------------
    BATCH_DATE = "{{ dag_run.conf['batch_date'] if dag_run else '2026/04/11/20' }}"
    BATCH_ID = "{{ dag_run.conf['batch_date'].replace('/', '') if dag_run else '2026041120' }}"

    # -------------------------
    # STEP 1: S3 RAW → PROCESSED (USING AWS HOOK)
    # -------------------------
    def move_s3_data():
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        bucket = "s3-ecommerce-us"

        # Orders
        s3.copy_objects(
            source_bucket_name=bucket,
            dest_bucket_name=bucket,
            prefix="firehose/raw/orders/" + BATCH_DATE + "/",
            dest_prefix="firehose/processed/orders/" + BATCH_ID + "/",
        )

        # Customers
        s3.copy_objects(
            source_bucket_name=bucket,
            dest_bucket_name=bucket,
            prefix="firehose/raw/customers/" + BATCH_DATE + "/",
            dest_prefix="firehose/processed/customers/" + BATCH_ID + "/",
        )

    s3_transform_task = PythonOperator(
        task_id="s3_raw_to_processed",
        python_callable=move_s3_data,
    )

    # -------------------------
    # STEP 2: LOAD ORDERS TO SNOWFLAKE
    # -------------------------
    load_orders = SQLExecuteQueryOperator(
        task_id="load_orders",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        COPY INTO RETAIL_DB.RETAIL_SCHEMA.ORDERS_RAW
        FROM @ORDERS_STAGE
        PATTERN='.*{BATCH_ID}.*'
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"');
        """,
    )

    # -------------------------
    # STEP 3: LOAD CUSTOMERS TO SNOWFLAKE
    # -------------------------
    load_customers = SQLExecuteQueryOperator(
        task_id="load_customers",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        COPY INTO RETAIL_DB.RETAIL_SCHEMA.CUSTOMERS_RAW
        FROM @CUSTOMERS_STAGE
        PATTERN='.*{BATCH_ID}.*'
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"');
        """,
    )

    # -------------------------
    # STEP 4: TRANSFORMATION IN SNOWFLAKE
    # -------------------------
    transformation = SQLExecuteQueryOperator(
        task_id="snowflake_transformation",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        INSERT INTO RETAIL_DB.RETAIL_SCHEMA.ORDER_CUSTOMER_DATE_PRICE
        SELECT 
            c.C_NAME,
            o.O_ORDERDATE,
            SUM(o.O_TOTALPRICE),
            c.C_BATCH_ID
        FROM ORDERS_RAW o
        JOIN CUSTOMERS_RAW c 
            ON o.O_CUSTKEY = c.C_CUSTKEY
            AND o.O_BATCH_ID = c.C_BATCH_ID
        WHERE o.O_ORDERSTATUS = 'F'
        GROUP BY c.C_NAME, o.O_ORDERDATE, c.C_BATCH_ID;
        """,
    )

    # -------------------------
    # FLOW
    # -------------------------
    s3_transform_task >> [load_orders, load_customers] >> transformation
