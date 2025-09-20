from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pendulum
import random
from datetime import timedelta

FAKESTORE_API = "https://fakestoreapi.com/products"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_fetch_api",
    default_args=default_args,
    start_date=pendulum.now().subtract(days=1),
    schedule="0 6 * * *",
    catchup=False
) as dag:

    @task
    def fetch_products():
        pg_hook = PostgresHook(postgres_conn_id="pg_connection")
        response = requests.get(FAKESTORE_API)
        data = response.json()

        rows = []
        for item in data:
            rows.append(
                (
                    str(item["id"]),
                    item["title"],
                    item.get("category", "Misc"),
                    float(item["price"])
                )
            )

        pg_hook.insert_rows(
            table="staging_products",
            rows=rows,
            target_fields=["product_id", "name", "category", "price"],
            # replace=True
        )

    @task
    def fetch_orders():
        pg_hook = PostgresHook(postgres_conn_id="pg_connection")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT product_id, price FROM staging_products;")
        product_prices = {row[0]: row[1] for row in cursor.fetchall()}
        product_ids = list(product_prices.keys())

        num = random.randint(5, 10)
        customer_ids = [str(random.randint(10000, 99999)) for _ in range(num)]
        customers = []
        for customer_id in customer_ids:
            customers.append((
                customer_id,
                f"Customer_{customer_id}",
                f"customer{customer_id}@example.com",
                random.choice([
                    "India", "China", "United Kingdom", "Thailand",
                    "Germany", "Spain", "France", "Japan", "Ireland",
                    "Italy", "Portugal", "Australia", "Denmark", "Sweden"
                ]),
                (datetime.today() - timedelta(days=random.randint(1, 2000))).date()
            ))

        pg_hook.insert_rows(
            table="staging_customers",
            rows=customers,
            target_fields=["customer_id", "name", "email", "country", "signup_date"]
        )

        orders = []
        order_items = []

        for _ in range(10):
            order_id = str(random.randint(10000, 99999))
            cust_id = random.choice(customer_ids)
            order_date = datetime.today() - timedelta(days=random.randint(0, 10))
            total_amount = 0

            num_items = random.randint(1, 4)
            for _ in range(num_items):
                product_id = random.choice(product_ids)
                quantity = random.randint(1, 5)
                price = product_prices[product_id]
                total_amount += quantity * price
                order_items.append((
                    order_id,
                    product_id,
                    quantity,
                    price
                ))

            orders.append((
                order_id,
                cust_id,
                order_date,
                "Completed",
                total_amount
            ))

        pg_hook.insert_rows(
            table="staging_orders",
            rows=orders,
            target_fields=["order_id", "customer_id", "order_date", "order_status", "total_amount"]
        )

        pg_hook.insert_rows(
            table="staging_order_items",
            rows=order_items,
            target_fields=["order_id", "product_id", "quantity", "price"]
        )

        cursor.close()
        conn.close()

    fetch_products() >> fetch_orders()
