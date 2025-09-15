CREATE DATABASE instacart_db;
USE DATABASE instacart_db;

CREATE SCHEMA raw_data;
CREATE SCHEMA analytics;





//table creation 
CREATE OR REPLACE TABLE raw_data.orders (
    order_id STRING,
    user_id STRING,
    eval_set STRING,
    order_number INT,
    order_dow INT,
    order_hour_of_day INT,
    days_since_prior_order INT
);

CREATE OR REPLACE TABLE raw_data.order_products_prior (
    order_id STRING,
    product_id STRING,
    add_to_cart_order INT,
    reordered INT
);

CREATE OR REPLACE TABLE raw_data.order_products_train (
    order_id STRING,
    product_id STRING,
    add_to_cart_order INT,
    reordered INT
);

CREATE OR REPLACE TABLE raw_data.products (
    product_id STRING,
    product_name STRING,
    aisle_id STRING,
    department_id STRING
);

CREATE OR REPLACE TABLE raw_data.aisles (
    aisle_id STRING,
    aisle_name STRING
);

CREATE OR REPLACE TABLE raw_data.departments (
    department_id STRING,
    department_name STRING
);


-- Create storage integration
CREATE STORAGE INTEGRATION s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::905418053002:role/snow_insta'
STORAGE_ALLOWED_LOCATIONS = ('s3://vininsta/');



DESC INTEGRATION s3_integration;


// file format
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ','
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE
TRIM_SPACE = TRUE;


// ctreate external stage

CREATE OR REPLACE STAGE s3_stage
URL = 's3://vininsta/'
STORAGE_INTEGRATION = s3_integration
FILE_FORMAT =csv_format;

list @s3_stage;

//load

-- Load orders
COPY INTO raw_data.orders
FROM @s3_stage/orders.csv
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Load order_products_prior
COPY INTO raw_data.order_products_prior
FROM @s3_stage/order_products__prior.csv
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Load order_products_train
COPY INTO raw_data.order_products_train
FROM @s3_stage/order_products__train.csv
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Load products
COPY INTO raw_data.products
FROM @s3_stage/products.csv
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Load aisles
COPY INTO raw_data.aisles
FROM @s3_stage/aisles.csv
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Load departments
COPY INTO raw_data.departments
FROM @s3_stage/departments.csv
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';


//remove duplicates and null
-- Orders
CREATE OR REPLACE TABLE analytics.orders_clean AS
SELECT DISTINCT *
FROM raw_data.orders
WHERE order_id IS NOT NULL;

-- Order Products Prior
CREATE OR REPLACE TABLE analytics.order_products_prior_clean AS
SELECT DISTINCT *
FROM raw_data.order_products_prior
WHERE order_id IS NOT NULL AND product_id IS NOT NULL;

-- Order Products Train
CREATE OR REPLACE TABLE analytics.order_products_train_clean AS
SELECT DISTINCT *
FROM raw_data.order_products_train
WHERE order_id IS NOT NULL AND product_id IS NOT NULL;

-- Products
CREATE OR REPLACE TABLE analytics.products_clean AS
SELECT DISTINCT *
FROM raw_data.products
WHERE product_id IS NOT NULL;

-- Aisles (dimension)
CREATE OR REPLACE TABLE analytics.aisles_clean AS
SELECT DISTINCT *
FROM raw_data.aisles
WHERE aisle_id IS NOT NULL;

-- Departments (dimension)
CREATE OR REPLACE TABLE analytics.departments_clean AS
SELECT DISTINCT *
FROM raw_data.departments
WHERE department_id IS NOT NULL;



// dimension table 
// product

CREATE OR REPLACE TABLE analytics.dim_products AS
SELECT 
    p.product_id,
    p.product_name,
    a.aisle_name AS aisle,
    d.department_name AS department
FROM analytics.products_clean p
JOIN analytics.aisles_clean a ON p.aisle_id = a.aisle_id
JOIN analytics.departments_clean d ON p.department_id = d.department_id;

//user

CREATE OR REPLACE TABLE analytics.dim_users AS
SELECT DISTINCT user_id
FROM analytics.orders_clean;


//fact table
CREATE OR REPLACE TABLE analytics.fact_orders AS
SELECT 
    op.order_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    o.days_since_prior_order,
    op.product_id,
    op.add_to_cart_order,
    op.reordered,
    p.product_name,
    p.aisle,
    p.department
FROM analytics.order_products_prior_clean op
JOIN analytics.orders_clean o
    ON op.order_id = o.order_id
JOIN analytics.dim_products p
    ON op.product_id = p.product_id;


// Aggregated Analytics Tables


CREATE OR REPLACE TABLE analytics.top_products AS
SELECT 
    product_id,
    product_name,
    COUNT(DISTINCT order_id) AS num_orders,
    COUNT(*) AS total_times_ordered,
    SUM(reordered) AS total_reordered
FROM analytics.fact_orders
GROUP BY product_id, product_name
ORDER BY total_times_ordered DESC;






CREATE OR REPLACE TABLE analytics.user_stats AS
SELECT 
    user_id,
    COUNT(DISTINCT order_id) AS total_orders,
    AVG(days_since_prior_order) AS avg_days_between_orders,
    SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) AS total_reorders
FROM analytics.fact_orders
GROUP BY user_id;




CREATE OR REPLACE TABLE analytics.department_stats AS
SELECT 
    department,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_products_ordered
FROM analytics.fact_orders
GROUP BY department
ORDER BY total_products_ordered DESC;
