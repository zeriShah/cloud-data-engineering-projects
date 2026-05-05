from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {
    "owner": "qasimhassan",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_orders_datapipeline_dynamic_batch_id",
    description="Runs data pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger DAG
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["snowflake", "batch"],
) as dag:

    # Use BATCH_DATE from dag_run.conf if provided, else default to logical_date
    BATCH_DATE = "{{ dag_run.conf['batch_date'] if dag_run else logical_date.strftime('%Y/%m/%d/%H') }}"
    BATCH_ID = "{{ dag_run.conf.get('batch_date', ts_nodash) }}"

    # ---------------- Bash Tasks ---------------- #

    bash_start = BashOperator(
        task_id="start",
        bash_command="echo Starting pipeline",
    )

    post_task = BashOperator(
        task_id="post_dbt",
        bash_command="echo Pipeline completed",
    )

    # Customers: landing -> processing
    customer_landing_to_processing = BashOperator(
        task_id="customer_landing_to_processing",
        bash_command=(
            "aws s3 ls s3://ecommerece-datapipeline-qh/firehouse/customers/landing/"
            f"{BATCH_DATE}/ && "
            "aws s3 mv "
            f"s3://ecommerece-datapipeline-qh/firehouse/customers/landing/{BATCH_DATE}/ "
            f"s3://ecommerece-datapipeline-qh/firehouse/customers/processing/{BATCH_ID}/ "
            "--recursive || echo 'No data for this batch'"
        ),
    )

    customer_processing_to_processed = BashOperator(
        task_id="customer_processing_to_processed",
        bash_command=(
            f"aws s3 mv "
            f"s3://ecommerece-datapipeline-qh/firehouse/customers/processing/{BATCH_ID}/ "
            f"s3://ecommerece-datapipeline-qh/firehouse/customers/processed/{BATCH_ID}/ "
            "--recursive"
        ),
    )

    # Orders: landing -> processing
    orders_landing_to_processing = BashOperator(
        task_id="orders_landing_to_processing",
        bash_command=(
            "aws s3 ls s3://ecommerece-datapipeline-qh/firehouse/orders/landing/"
            f"{BATCH_DATE}/ && "
            "aws s3 mv "
            f"s3://ecommerece-datapipeline-qh/firehouse/orders/landing/{BATCH_DATE}/ "
            f"s3://ecommerece-datapipeline-qh/firehouse/orders/processing/{BATCH_ID}/ "
            "--recursive || echo 'No data for this batch'"
        ),
    )

    orders_processing_to_processed = BashOperator(
        task_id="orders_processing_to_processed",
        bash_command=(
            f"aws s3 mv "
            f"s3://ecommerece-datapipeline-qh/firehouse/orders/processing/{BATCH_ID}/ "
            f"s3://ecommerece-datapipeline-qh/firehouse/orders/processed/{BATCH_ID}/ "
            "--recursive"
        ),
    )

    # ---------------- Snowflake SQL ---------------- #

    snowflake_orders_sql = SQLExecuteQueryOperator(
        task_id="snowflake_raw_insert_order",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        COPY INTO RETAIL_DB.RETAIL_SCHEMA.ORDERS_RAW
        FROM (
            SELECT 
            '{BATCH_ID}',t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9
            FROM @ORDERS_RAW_STAGE t
        );
        """,
    )

    snowflake_customers_sql = SQLExecuteQueryOperator(
        task_id="snowflake_raw_insert_customers",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        COPY INTO RETAIL_DB.RETAIL_SCHEMA.CUSTOMERS_RAW
        FROM (
            SELECT '{BATCH_ID}', t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8
            FROM @CUSTOMER_RAW_STAGE t
        );
        """,
    )

    snowflake_transformation = SQLExecuteQueryOperator(
        task_id="snowflake_order_customers_small_transformation",
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
        GROUP BY C_NAME, O_ORDERDATE, c.C_BATCH_ID
        ORDER BY O_ORDERDATE;
        """,
    )

    # ---------------- DAG Dependencies ---------------- #

    bash_start >> [
        orders_landing_to_processing >> snowflake_orders_sql >> orders_processing_to_processed,
        customer_landing_to_processing >> snowflake_customers_sql >> customer_processing_to_processed,
    ] >> snowflake_transformation >> post_task