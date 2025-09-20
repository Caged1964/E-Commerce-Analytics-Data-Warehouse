from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pendulum
import pandas as pd
import os

BASE_PATH = os.path.join(os.environ.get("AIRFLOW_HOME","/opt/airflow"),"include")

def load_to_staging(table_name, csv_file, **kwargs):
    ALLOWED_TABLES = {"staging_customers","staging_products","staging_orders","staging_order_items"}
    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table_name")

    pg_hook = PostgresHook(postgres_conn_id='pg_connection')
    df = pd.read_csv(csv_file)

    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    columns = list(df.columns)
    placeholders = ",".join(["%s"] * len(columns))
    colnames = ",".join(columns)

    # If table is staging_order_items, do plain insert
    if table_name == "staging_order_items":
        for _, row in df.iterrows():
            cursor.execute(
                f"INSERT INTO {table_name} ({colnames}) VALUES ({placeholders})",
                tuple(row)
            )
    else:
        pk = columns[0]
        updates = ",".join([f"{col}=EXCLUDED.{col}" for col in columns[1:]])
        for _, row in df.iterrows():
            cursor.execute(
                f"""
                INSERT INTO {table_name} ({colnames})
                VALUES ({placeholders})
                ON CONFLICT ({pk})
                DO UPDATE SET {updates};
                """,
                tuple(row)
            )
    conn.commit()

## Defining DAG 

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='bootstrap_load_dag',
    default_args=default_args,
    start_date = pendulum.now().subtract(days=1),
    schedule=None,
    catchup=False
) as dag:
    load_customers_csv = PythonOperator(
        task_id='load_customers_csv',
        python_callable=load_to_staging,
        op_kwargs={"table_name":"staging_customers",
                   "csv_file": os.path.join(BASE_PATH,"customers.csv")}
    )

    load_products_csv = PythonOperator(
        task_id='load_products_csv',
        python_callable=load_to_staging,
        op_kwargs={"table_name":"staging_products",
                   "csv_file": os.path.join(BASE_PATH,"products.csv")}
    )

    load_orders_csv = PythonOperator(
        task_id='load_orders_csv',
        python_callable=load_to_staging,
        op_kwargs={"table_name":"staging_orders",
                   "csv_file": os.path.join(BASE_PATH,"orders.csv")}
    )

    load_order_items_csv = PythonOperator(
        task_id='load_order_items_csv',
        python_callable=load_to_staging,
        op_kwargs={"table_name":"staging_order_items",
                   "csv_file": os.path.join(BASE_PATH,"order_items.csv")}
    )

    
    ## Dependencies
    [load_customers_csv, load_products_csv] >> load_orders_csv >> load_order_items_csv