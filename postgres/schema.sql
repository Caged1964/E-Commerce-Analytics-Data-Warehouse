-- STAGING TABLES (for raw data)
CREATE TABLE IF NOT EXISTS staging_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    order_status VARCHAR(50),
    total_amount NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS staging_order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS staging_customers (
    customer_id VARCHAR(50),
    name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(100),
    signup_date DATE
);

CREATE TABLE IF NOT EXISTS staging_products (
    product_id VARCHAR(50),
    name VARCHAR(100),
    category VARCHAR(255),
    price NUMERIC(10,2)
);

-- DIMENSION TABLES 
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(100),
    signup_date DATE
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS dim_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    status VARCHAR(50),
    total_amount NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    day INT, 
    month INT,
    year INT
);

-- FACT TABLE
CREATE TABLE IF NOT EXISTS fact_sales (
    order_item_id INT PRIMARY KEY,
    order_id VARCHAR(50),
    customer_id VARCHAR(50) REFERENCES dim_customers(customer_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    date_id DATE REFERENCES dim_date(date_id),
    quantity INT,
    unit_price NUMERIC(10,2),
    total_amount NUMERIC(12,2)
);