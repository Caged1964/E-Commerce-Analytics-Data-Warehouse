from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import  datetime, timedelta
import pendulum

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_transform_analytics",
    default_args=default_args,
    start_date=pendulum.now().subtract(days=1),
    schedule="@daily",
    catchup=False
) as dag:

    @task
    def load_dim_customers():
        pg_hook = PostgresHook(postgres_conn_id="pg_connection")
        insert_query = """
        INSERT INTO dim_customers (customer_id, name, email, country, signup_date)
        WITH ranked_customers AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) AS rn
          FROM staging_customers
        )
        SELECT customer_id, name, email, country, signup_date
        FROM ranked_customers
        WHERE rn = 1
        ON CONFLICT (customer_id) DO UPDATE
        SET name = EXCLUDED.name,
            email = EXCLUDED.email,
            country = EXCLUDED.country,
            signup_date = EXCLUDED.signup_date;
        """
        pg_hook.run(insert_query)

    @task
    def load_dim_products():
        pg_hook = PostgresHook(postgres_conn_id="pg_connection")
        insert_query = """
        INSERT INTO dim_products (product_id, product_name, category, price)
        WITH ranked_products AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price DESC) AS rn
          FROM staging_products
        )
        SELECT product_id, name, category, price
        FROM ranked_products
        WHERE rn = 1
        ON CONFLICT (product_id) DO UPDATE
        SET product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            price = EXCLUDED.price;
        """
        pg_hook.run(insert_query)

    @task
    def load_dim_orders():
        pg_hook = PostgresHook(postgres_conn_id="pg_connection")
        insert_query = """
        INSERT INTO dim_orders (order_id, customer_id, order_date, status, total_amount)
        WITH ranked_orders AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
          FROM staging_orders
        )
        SELECT order_id, customer_id, order_date, order_status, total_amount
        FROM ranked_orders
        WHERE rn = 1
        ON CONFLICT (order_id) DO UPDATE
        SET customer_id = EXCLUDED.customer_id,
            order_date = EXCLUDED.order_date,
            status = EXCLUDED.status,
            total_amount = EXCLUDED.total_amount;
        """
        pg_hook.run(insert_query)

    @task
    def load_dim_date():
        pg_hook = PostgresHook(postgres_conn_id="pg_connection")
        insert_query = """
        INSERT INTO dim_date (date_id, day, month, year)
        SELECT DISTINCT order_date::date,
               EXTRACT(DAY FROM order_date),
               EXTRACT(MONTH FROM order_date),
               EXTRACT(YEAR FROM order_date)
        FROM staging_orders
        ON CONFLICT (date_id) DO NOTHING;
        """
        pg_hook.run(insert_query)

    @task
    def load_fact_sales():
        pg_hook = PostgresHook(postgres_conn_id="pg_connection")
        insert_query = """
        INSERT INTO fact_sales (order_item_id, order_id, customer_id, product_id, date_id, quantity, unit_price, total_amount)
        SELECT soi.order_item_id,
               soi.order_id,
               so.customer_id,
               soi.product_id,
               so.order_date::date,
               soi.quantity,
               soi.price,
               (soi.quantity * soi.price)
        FROM staging_order_items soi
        JOIN staging_orders so ON soi.order_id = so.order_id
        ON CONFLICT (order_item_id) DO UPDATE
        SET order_id = EXCLUDED.order_id,
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            date_id = EXCLUDED.date_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            total_amount = EXCLUDED.total_amount;
        """
        pg_hook.run(insert_query)

    load_dim_customers() >> load_dim_products() >> load_dim_orders() >> load_dim_date() >> load_fact_sales()
