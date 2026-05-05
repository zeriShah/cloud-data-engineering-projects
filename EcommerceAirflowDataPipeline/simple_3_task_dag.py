from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {
    "owner": "qasimhassan",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def move_s3_files(**kwargs):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    # Connects natively using Airflow's "aws_default" connection
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3 = s3_hook.get_conn()
    
    moves = kwargs['templates_dict']['moves']
    
    for move in moves:
        src_uri = move['src']
        dest_uri = move['dest']
        
        src_bucket = src_uri.replace("s3://", "").split("/")[0]
        src_prefix = src_uri.replace(f"s3://{src_bucket}/", "")
        dest_bucket = dest_uri.replace("s3://", "").split("/")[0]
        dest_prefix = dest_uri.replace(f"s3://{dest_bucket}/", "")
        
        print(f"Moving from {src_prefix} to {dest_prefix}")
        
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=src_bucket, Prefix=src_prefix)
        
        moved = 0
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    src_key = obj['Key']
                    new_key = src_key.replace(src_prefix, dest_prefix, 1)
                    
                    s3.copy_object(
                        Bucket=dest_bucket,
                        CopySource={'Bucket': src_bucket, 'Key': src_key},
                        Key=new_key
                    )
                    s3.delete_object(Bucket=src_bucket, Key=src_key)
                    moved += 1
        
        print(f"Successfully moved {moved} objects from {src_uri}.")

with DAG(
    dag_id="ecommerce_3_task_pipeline",
    description="3-task DAG with pure Python S3 Operations",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["snowflake", "batch", "python"],
) as dag:

    # Safely get batch_date from dag_run.conf, falling back to logical_date / ts_nodash
    BATCH_DATE = "{{ dag_run.conf.get('batch_date', logical_date.strftime('%Y/%m/%d/%H')) if dag_run and dag_run.conf else logical_date.strftime('%Y/%m/%d/%H') }}"
    BATCH_ID = "{{ dag_run.conf.get('batch_date', ts_nodash) if dag_run and dag_run.conf else ts_nodash }}"

    # ---------------- Task 1: Landing -> Processing (S3) ---------------- #
    move_landing_to_processing = PythonOperator(
        task_id="move_landing_to_processing",
        python_callable=move_s3_files,
        templates_dict={
            "moves": [
                {
                    "src": f"s3://s3-ecommerce-us/firehose/landing/customers/{BATCH_DATE}/",
                    "dest": f"s3://s3-ecommerce-us/firehose/processing/customers/{BATCH_ID}/"
                },
                {
                    "src": f"s3://s3-ecommerce-us/firehose/landing/orders/{BATCH_DATE}/",
                    "dest": f"s3://s3-ecommerce-us/firehose/processing/orders/{BATCH_ID}/"
                }
            ]
        }
    )

    # ---------------- Task 2: Load to Snowflake & Transform ---------------- #
    # This runs COPY INTO for both tables, then runs the JOIN transformation, as a single Airflow task.
    snowflake_load_and_transform = SQLExecuteQueryOperator(
        task_id="snowflake_load_and_transform",
        conn_id=SNOWFLAKE_CONN_ID,
        split_statements=True,  
        sql=f"""
        -- 1) Load Orders into RAW Table
        COPY INTO AIRFLOW_DB.AIRFLOW_SCHEMA.ORDERS_RAW
        FROM (
            SELECT '{BATCH_ID}', t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9
            FROM @AIRFLOW_DB.AIRFLOW_SCHEMA.ecommerce_stage/processing/orders/{BATCH_ID}/ t
        )
        ON_ERROR = 'CONTINUE';

        -- 2) Load Customers into RAW Table
        COPY INTO AIRFLOW_DB.AIRFLOW_SCHEMA.CUSTOMERS_RAW
        FROM (
            SELECT '{BATCH_ID}', t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8
            FROM @AIRFLOW_DB.AIRFLOW_SCHEMA.ecommerce_stage/processing/customers/{BATCH_ID}/ t
        )
        ON_ERROR = 'CONTINUE';

        -- 3) Run Data Transformation Logic
        INSERT INTO AIRFLOW_DB.AIRFLOW_SCHEMA.ORDER_CUSTOMER_DATE_PRICE
        SELECT 
            c.C_NAME,
            o.O_ORDERDATE,
            SUM(o.O_TOTALPRICE),
            c.C_BATCH_ID
        FROM AIRFLOW_DB.AIRFLOW_SCHEMA.ORDERS_RAW o
        JOIN AIRFLOW_DB.AIRFLOW_SCHEMA.CUSTOMERS_RAW c 
          ON o.O_CUSTKEY = c.C_CUSTKEY
         AND o.O_BATCH_ID = c.C_BATCH_ID
        WHERE o.O_ORDERSTATUS = 'F'
          AND o.O_BATCH_ID = '{BATCH_ID}'
        GROUP BY C_NAME, O_ORDERDATE, c.C_BATCH_ID
        ORDER BY O_ORDERDATE;
        """,
    )

    # ---------------- Task 3: Processing -> Processed (S3) ---------------- #
    move_processing_to_processed = PythonOperator(
        task_id="move_processing_to_processed",
        python_callable=move_s3_files,
        templates_dict={
            "moves": [
                {
                    "src": f"s3://s3-ecommerce-us/firehose/processing/customers/{BATCH_ID}/",
                    "dest": f"s3://s3-ecommerce-us/firehose/processed/customers/{BATCH_ID}/"
                },
                {
                    "src": f"s3://s3-ecommerce-us/firehose/processing/orders/{BATCH_ID}/",
                    "dest": f"s3://s3-ecommerce-us/firehose/processed/orders/{BATCH_ID}/"
                }
            ]
        }
    )

    # ---------------- DAG Dependencies ---------------- #
    move_landing_to_processing >> snowflake_load_and_transform >> move_processing_to_processed
