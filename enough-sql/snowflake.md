# Snowflake Learning Path: Beginner to Expert

*A comprehensive guide for mastering Snowflake Data Cloud*

---
## chapter 1 to 6 in details 
## chapter 7 to 10 conceptually then first attempt
## Table of Contents
1. [Beginner Level](#beginner-level)
2. [Intermediate Level](#intermediate-level)
3. [Advanced Level](#advanced-level)
4. [Expert Level](#expert-level)
5. [Certification Path](#certification-path)
6. [Hands-On Projects](#hands-on-projects)

---

## BEGINNER LEVEL

### 1. Snowflake Fundamentals

#### 1.1 Core Concepts
- What is Snowflake and why it matters
- Cloud data warehouse vs traditional databases
- Snowflake's unique architecture (multi-cluster shared data)
- Key value propositions: separation of storage and compute, instant elasticity, zero management
- Snowflake editions: Standard, Enterprise, Business Critical, Virtual Private Snowflake (VPS)
- Understanding Snowflake pricing model: storage costs, compute costs, data transfer costs

#### 1.2 Architecture Overview
- **Storage Layer**: centralized, scalable, columnar storage
- **Compute Layer**: virtual warehouses (independent compute clusters)
- **Cloud Services Layer**: authentication, metadata management, query optimization, infrastructure management
- Understanding the separation of concerns
- How Snowflake scales independently for storage and compute
- Multi-cloud support (AWS, Azure, GCP)

#### 1.3 Account Structure
- Organizations and accounts
- Regions and cloud providers
- Account identifiers and locators
- Account naming conventions
- Understanding account URLs

#### 1.4 Getting Started
- Creating a Snowflake trial account
- Navigating the Snowflake Web UI (Snowsight)
- Understanding the Classic Console vs Snowsight
- Basic navigation: Worksheets, Databases, Data, Dashboards, Monitoring
- Setting up user preferences and notifications

### 2. Database Objects and SQL Basics

#### 2.1 Database Hierarchy
- **Databases**: top-level containers
- **Schemas**: logical groupings within databases
- **Tables**: structured data storage
- **Views**: saved SELECT queries
- Fully qualified names: `DATABASE.SCHEMA.OBJECT`
- Information Schema and Account Usage views

#### 2.2 Creating Databases and Schemas
```sql
-- Creating databases
CREATE DATABASE my_database;
CREATE DATABASE IF NOT EXISTS my_database;
CREATE TRANSIENT DATABASE temp_db;  -- Lower cost, no fail-safe
CREATE DATABASE my_db COMMENT='Production database';

-- Creating schemas
CREATE SCHEMA my_schema;
CREATE SCHEMA IF NOT EXISTS my_database.my_schema;
CREATE TRANSIENT SCHEMA temp_schema;

-- Showing and using
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE my_database;
USE DATABASE my_database;
USE SCHEMA my_schema;
```

#### 2.3 Table Types and Creation
```sql
-- Permanent tables (default, 7 days fail-safe)
CREATE TABLE customers (
    customer_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    created_date DATE
);

-- Temporary tables (session-scoped, no fail-safe)
CREATE TEMPORARY TABLE temp_results (
    id INTEGER,
    value VARCHAR
);

-- Transient tables (no fail-safe, lower cost)
CREATE TRANSIENT TABLE staging_data (
    raw_data VARIANT
);

-- Tables with constraints
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE(),
    status VARCHAR(20) DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Tables with clustering keys
CREATE TABLE events (
    event_id INTEGER,
    event_date DATE,
    event_type VARCHAR(50),
    data VARIANT
) CLUSTER BY (event_date, event_type);
```

#### 2.4 Data Types
- **Numeric**: NUMBER, DECIMAL, NUMERIC, INT, INTEGER, BIGINT, SMALLINT, TINYINT, BYTEINT, FLOAT, DOUBLE
- **String**: VARCHAR, CHAR, STRING, TEXT, BINARY, VARBINARY
- **Date/Time**: DATE, DATETIME, TIME, TIMESTAMP, TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ
- **Semi-structured**: VARIANT, OBJECT, ARRAY
- **Boolean**: BOOLEAN
- **Geospatial**: GEOGRAPHY, GEOMETRY
- Understanding data type conversions and casting

#### 2.5 Basic SQL Operations
```sql
-- INSERT
INSERT INTO customers VALUES (1, 'John', 'Doe', 'john@example.com', '2024-01-01');
INSERT INTO customers (customer_id, first_name, last_name) 
VALUES (2, 'Jane', 'Smith');

-- SELECT
SELECT * FROM customers;
SELECT first_name, last_name FROM customers WHERE customer_id = 1;
SELECT COUNT(*) FROM customers;

-- UPDATE
UPDATE customers SET email = 'newemail@example.com' WHERE customer_id = 1;

-- DELETE
DELETE FROM customers WHERE customer_id = 2;

-- TRUNCATE (faster than DELETE for all rows)
TRUNCATE TABLE temp_results;

-- DROP
DROP TABLE IF EXISTS temp_results;
```

### 3. Virtual Warehouses

#### 3.1 Understanding Virtual Warehouses
- What is a virtual warehouse (compute cluster)
- Warehouse sizes: X-Small to 6X-Large
- Understanding credits consumption by size
- When warehouses are running vs suspended
- Multi-cluster warehouses (Enterprise Edition+)

#### 3.2 Creating and Managing Warehouses
```sql
-- Creating warehouses
CREATE WAREHOUSE my_warehouse
    WITH WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL jobs';

-- Different sizes
CREATE WAREHOUSE xs_warehouse WAREHOUSE_SIZE = 'X-SMALL';
CREATE WAREHOUSE large_warehouse WAREHOUSE_SIZE = 'LARGE';

-- Modifying warehouses
ALTER WAREHOUSE my_warehouse SET WAREHOUSE_SIZE = 'MEDIUM';
ALTER WAREHOUSE my_warehouse SET AUTO_SUSPEND = 600;
ALTER WAREHOUSE my_warehouse SUSPEND;
ALTER WAREHOUSE my_warehouse RESUME;

-- Multi-cluster warehouses (Enterprise+)
CREATE WAREHOUSE multi_wh
    WITH WAREHOUSE_SIZE = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- Using warehouses
USE WAREHOUSE my_warehouse;

-- Showing warehouses
SHOW WAREHOUSES;
DESC WAREHOUSE my_warehouse;

-- Dropping warehouses
DROP WAREHOUSE IF EXISTS my_warehouse;
```

#### 3.3 Warehouse Best Practices
- Right-sizing warehouses for workload
- Setting appropriate auto-suspend times (300-600 seconds recommended)
- Using auto-resume to save costs
- Dedicating warehouses for different workloads (ETL vs BI)
- Understanding warehouse caching (result cache, metadata cache, local disk cache)

### 4. Loading Data into Snowflake

#### 4.1 Data Loading Options Overview
- COPY INTO command (most common)
- Snowpipe (continuous, automated loading)
- INSERT statements (small amounts)
- Third-party ETL tools (Fivetran, Matillion, etc.)
- Partner connectors (Kafka, Spark, etc.)

#### 4.2 Stages (Internal and External)
```sql
-- Internal stages (data stored in Snowflake)
-- User stage (@ prefix)
PUT file:///local/path/data.csv @~/staged;
LIST @~;

-- Table stage (@%table_name)
PUT file:///local/path/data.csv @%my_table;
LIST @%my_table;

-- Named internal stage
CREATE STAGE my_internal_stage;
PUT file:///local/path/data.csv @my_internal_stage;
LIST @my_internal_stage;

-- External stages (S3, Azure Blob, GCS)
-- S3 example
CREATE STAGE my_s3_stage
    URL = 's3://mybucket/path/'
    CREDENTIALS = (AWS_KEY_ID = 'xxx' AWS_SECRET_KEY = 'xxx');

-- Azure example
CREATE STAGE my_azure_stage
    URL = 'azure://myaccount.blob.core.windows.net/mycontainer/path/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'xxx');

-- GCS example
CREATE STAGE my_gcs_stage
    URL = 'gcs://mybucket/path/'
    STORAGE_INTEGRATION = my_gcs_integration;

-- Listing files in external stage
LIST @my_s3_stage;
```

#### 4.3 File Formats
```sql
-- CSV file format
CREATE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = 'AUTO'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- JSON file format
CREATE FILE FORMAT my_json_format
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = TRUE;

-- Parquet file format
CREATE FILE FORMAT my_parquet_format
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';

-- Avro file format
CREATE FILE FORMAT my_avro_format
    TYPE = 'AVRO'
    COMPRESSION = 'AUTO';

-- ORC file format
CREATE FILE FORMAT my_orc_format
    TYPE = 'ORC';

-- Showing file formats
SHOW FILE FORMATS;
```

#### 4.4 COPY INTO Command
```sql
-- Basic COPY INTO
COPY INTO my_table
FROM @my_stage/file.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Using named file format
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

-- Pattern matching
COPY INTO my_table
FROM @my_stage
PATTERN = '.*sales_2024.*\\.csv';

-- With column mapping
COPY INTO my_table (col1, col2, col3)
FROM (
    SELECT $1, $2, $3
    FROM @my_stage/file.csv
)
FILE_FORMAT = (TYPE = 'CSV');

-- With transformations
COPY INTO my_table
FROM (
    SELECT 
        $1::INTEGER AS id,
        UPPER($2) AS name,
        TO_DATE($3, 'YYYY-MM-DD') AS date
    FROM @my_stage/file.csv
)
FILE_FORMAT = (TYPE = 'CSV');

-- Validation mode (test without loading)
COPY INTO my_table
FROM @my_stage
VALIDATION_MODE = 'RETURN_ERRORS';

COPY INTO my_table
FROM @my_stage
VALIDATION_MODE = 'RETURN_5_ROWS';

-- ON_ERROR options
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
ON_ERROR = 'CONTINUE';  -- SKIP_FILE, ABORT_STATEMENT, SKIP_FILE_<n>

-- Viewing load history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MY_TABLE',
    START_TIME => DATEADD(HOURS, -24, CURRENT_TIMESTAMP())
));
```

### 5. Querying Data

#### 5.1 Basic SELECT Statements
```sql
-- Simple queries
SELECT * FROM customers;
SELECT first_name, last_name FROM customers;
SELECT DISTINCT country FROM customers;
SELECT * FROM customers LIMIT 10;

-- WHERE clause
SELECT * FROM orders WHERE order_date = '2024-01-01';
SELECT * FROM customers WHERE last_name LIKE 'Sm%';
SELECT * FROM products WHERE price BETWEEN 10 AND 50;
SELECT * FROM orders WHERE status IN ('pending', 'processing');

-- ORDER BY
SELECT * FROM customers ORDER BY last_name ASC;
SELECT * FROM orders ORDER BY order_date DESC, total_amount ASC;

-- Aggregations
SELECT COUNT(*) FROM customers;
SELECT COUNT(DISTINCT customer_id) FROM orders;
SELECT SUM(amount) FROM orders;
SELECT AVG(price) FROM products;
SELECT MIN(order_date), MAX(order_date) FROM orders;

-- GROUP BY
SELECT country, COUNT(*) as customer_count
FROM customers
GROUP BY country
ORDER BY customer_count DESC;

SELECT DATE_TRUNC('month', order_date) as month, SUM(amount)
FROM orders
GROUP BY month;

-- HAVING
SELECT country, COUNT(*) as cnt
FROM customers
GROUP BY country
HAVING COUNT(*) > 100;
```

#### 5.2 Joins
```sql
-- INNER JOIN
SELECT c.first_name, c.last_name, o.order_id, o.amount
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id;

-- LEFT JOIN
SELECT c.*, o.order_id
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;

-- RIGHT JOIN
SELECT c.*, o.order_id
FROM customers c
RIGHT JOIN orders o ON c.customer_id = o.customer_id;

-- FULL OUTER JOIN
SELECT c.*, o.order_id
FROM customers c
FULL OUTER JOIN orders o ON c.customer_id = o.customer_id;

-- CROSS JOIN
SELECT * FROM table1 CROSS JOIN table2;

-- Self join
SELECT e1.name as employee, e2.name as manager
FROM employees e1
LEFT JOIN employees e2 ON e1.manager_id = e2.employee_id;
```

#### 5.3 Subqueries and CTEs
```sql
-- Subquery in WHERE
SELECT * FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM orders WHERE amount > 1000
);

-- Subquery in SELECT
SELECT 
    customer_id,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) as order_count
FROM customers c;

-- Common Table Expressions (CTEs)
WITH high_value_customers AS (
    SELECT customer_id, SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 10000
)
SELECT c.*, hvc.total_spent
FROM customers c
JOIN high_value_customers hvc ON c.customer_id = hvc.customer_id;

-- Multiple CTEs
WITH 
monthly_sales AS (
    SELECT DATE_TRUNC('month', order_date) as month, SUM(amount) as revenue
    FROM orders
    GROUP BY month
),
avg_monthly AS (
    SELECT AVG(revenue) as avg_revenue
    FROM monthly_sales
)
SELECT * FROM monthly_sales
WHERE revenue > (SELECT avg_revenue FROM avg_monthly);
```

### 6. Views and Secure Views

#### 6.1 Standard Views
```sql
-- Creating views
CREATE VIEW customer_summary AS
SELECT 
    customer_id,
    first_name || ' ' || last_name AS full_name,
    email,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY customer_id, full_name, email;

-- Querying views
SELECT * FROM customer_summary WHERE total_spent > 1000;

-- Replacing views
CREATE OR REPLACE VIEW customer_summary AS
SELECT 
    customer_id,
    first_name,
    last_name,
    email
FROM customers;

-- Dropping views
DROP VIEW IF EXISTS customer_summary;
```

#### 6.2 Secure Views
```sql
-- Secure views hide definition from unauthorized users
CREATE SECURE VIEW sensitive_customer_data AS
SELECT 
    customer_id,
    first_name,
    last_name,
    CASE 
        WHEN CURRENT_ROLE() = 'ADMIN' THEN email
        ELSE 'REDACTED'
    END AS email
FROM customers;

-- Materialized views (covered in intermediate section)
```

---

## INTERMEDIATE LEVEL

### 7. Semi-Structured Data

#### 7.1 VARIANT Data Type
```sql
-- Creating table with VARIANT
CREATE TABLE events (
    event_id INTEGER,
    event_data VARIANT,
    event_timestamp TIMESTAMP_NTZ
);

-- Loading JSON data
COPY INTO events
FROM @my_stage/events.json
FILE_FORMAT = (TYPE = 'JSON');

-- Querying VARIANT data
SELECT 
    event_id,
    event_data:user_id::STRING AS user_id,
    event_data:event_type::STRING AS event_type,
    event_data:properties:page_url::STRING AS page_url
FROM events;

-- Flattening nested arrays
SELECT 
    event_id,
    f.value:product_id::STRING AS product_id,
    f.value:quantity::INTEGER AS quantity
FROM events,
LATERAL FLATTEN(input => event_data:items) f;
```

#### 7.2 JSON Functions
```sql
-- Extracting values
SELECT event_data:user_id::STRING FROM events;
SELECT event_data['properties']['page_url']::STRING FROM events;

-- GET_PATH
SELECT GET_PATH(event_data, 'properties.page_url') FROM events;

-- JSON_EXTRACT_PATH_TEXT
SELECT JSON_EXTRACT_PATH_TEXT(event_data, 'user_id') FROM events;

-- PARSE_JSON
SELECT PARSE_JSON('{"key": "value"}');

-- Checking existence
SELECT * FROM events 
WHERE event_data:properties:experiment_id IS NOT NULL;

-- Type checking
SELECT TYPEOF(event_data:user_id) FROM events;

-- Array functions
SELECT ARRAY_SIZE(event_data:items) FROM events;
SELECT ARRAY_CONTAINS('value'::VARIANT, event_data:tags) FROM events;
```

#### 7.3 FLATTEN Function
```sql
-- Basic FLATTEN
SELECT 
    e.event_id,
    f.value
FROM events e,
LATERAL FLATTEN(input => e.event_data:items) f;

-- FLATTEN with path
SELECT 
    e.event_id,
    f.key,
    f.value
FROM events e,
LATERAL FLATTEN(input => e.event_data:properties) f;

-- Multiple FLATTEN levels
SELECT 
    e.event_id,
    f1.value:product_id::STRING AS product_id,
    f2.value::STRING AS tag
FROM events e,
LATERAL FLATTEN(input => e.event_data:items) f1,
LATERAL FLATTEN(input => f1.value:tags) f2;

-- FLATTEN with OUTER => TRUE (include NULLs)
SELECT *
FROM events e,
LATERAL FLATTEN(input => e.event_data:items, OUTER => TRUE) f;
```

#### 7.4 OBJECT and ARRAY Types
```sql
-- Creating OBJECT
SELECT OBJECT_CONSTRUCT('name', 'John', 'age', 30);

-- Creating ARRAY
SELECT ARRAY_CONSTRUCT(1, 2, 3, 4, 5);

-- OBJECT_AGG
SELECT OBJECT_AGG(country, customer_count)
FROM (
    SELECT country, COUNT(*) AS customer_count
    FROM customers
    GROUP BY country
);

-- ARRAY_AGG
SELECT customer_id, ARRAY_AGG(order_id) AS order_ids
FROM orders
GROUP BY customer_id;
```

### 8. Time Travel and Fail-Safe

#### 8.1 Time Travel Basics
```sql
-- Query historical data (up to 90 days for Enterprise)
SELECT * FROM customers AT(OFFSET => -60*5);  -- 5 minutes ago
SELECT * FROM customers AT(TIMESTAMP => '2024-01-01 10:00:00'::TIMESTAMP);
SELECT * FROM customers BEFORE(STATEMENT => '01a8a23f-0000-1234-0000-000000000000');

-- Show changes
SELECT * FROM customers 
CHANGES(INFORMATION => DEFAULT)
AT(TIMESTAMP => '2024-01-01'::TIMESTAMP);

-- Clone from historical point
CREATE TABLE customers_backup CLONE customers 
AT(TIMESTAMP => '2024-01-01 00:00:00'::TIMESTAMP);
```

#### 8.2 Undropping Objects
```sql
-- Undrop table
DROP TABLE customers;
UNDROP TABLE customers;

-- Undrop schema
DROP SCHEMA my_schema;
UNDROP SCHEMA my_schema;

-- Undrop database
DROP DATABASE my_database;
UNDROP DATABASE my_database;

-- Show dropped objects
SHOW TABLES HISTORY;
SHOW SCHEMAS HISTORY;
```

#### 8.3 Data Retention Period
```sql
-- Setting retention period (0-90 days, Enterprise+)
ALTER TABLE customers SET DATA_RETENTION_TIME_IN_DAYS = 90;
ALTER SCHEMA my_schema SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER DATABASE my_db SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Checking retention
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE customers;
```

### 9. Zero-Copy Cloning

#### 9.1 Cloning Objects
```sql
-- Clone table
CREATE TABLE customers_dev CLONE customers;

-- Clone schema (including all tables)
CREATE SCHEMA dev_schema CLONE prod_schema;

-- Clone database
CREATE DATABASE dev_db CLONE prod_db;

-- Clone with swap
CREATE TABLE customers_new CLONE customers;
ALTER TABLE customers SWAP WITH customers_new;
DROP TABLE customers_new;

-- Clone from Time Travel
CREATE TABLE customers_backup CLONE customers 
AT(TIMESTAMP => DATEADD(day, -1, CURRENT_TIMESTAMP()));
```

#### 9.2 Clone Use Cases
- Creating instant dev/test environments
- Creating backups before major changes
- A/B testing with production data
- Quick recovery from mistakes
- Blue-green deployments

### 10. Streams and Change Data Capture (CDC)

#### 10.1 Creating Streams
```sql
-- Standard stream on table
CREATE STREAM customer_stream ON TABLE customers;

-- Stream with append-only mode
CREATE STREAM orders_stream ON TABLE orders 
APPEND_ONLY = TRUE;

-- Stream on view
CREATE STREAM view_stream ON VIEW customer_summary;

-- Stream on external table
CREATE STREAM ext_stream ON EXTERNAL TABLE my_ext_table;

-- Showing streams
SHOW STREAMS;
DESC STREAM customer_stream;
```

#### 10.2 Querying Streams
```sql
-- View changes in stream
SELECT * FROM customer_stream;

-- Metadata columns
SELECT 
    *,
    METADATA$ACTION,        -- INSERT, DELETE
    METADATA$ISUPDATE,      -- TRUE if part of UPDATE
    METADATA$ROW_ID         -- Unique row identifier
FROM customer_stream;

-- Processing changes
INSERT INTO customer_history
SELECT * FROM customer_stream
WHERE METADATA$ACTION = 'INSERT';

-- Stream is consumed after DML
-- Check stream status
SELECT SYSTEM$STREAM_HAS_DATA('customer_stream');
```

#### 10.3 Stream Patterns
```sql
-- CDC pattern
MERGE INTO target_table t
USING customer_stream s ON t.customer_id = s.customer_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN UPDATE SET
    t.first_name = s.first_name,
    t.last_name = s.last_name,
    t.email = s.email
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN INSERT
    (customer_id, first_name, last_name, email)
VALUES
    (s.customer_id, s.first_name, s.last_name, s.email);
```

### 11. Tasks (Scheduling)

#### 11.1 Creating Tasks
```sql
-- Simple scheduled task
CREATE TASK daily_cleanup
    WAREHOUSE = my_warehouse
    SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
DELETE FROM temp_table WHERE created_date < DATEADD(day, -7, CURRENT_DATE());

-- Task with interval
CREATE TASK hourly_aggregation
    WAREHOUSE = my_warehouse
    SCHEDULE = '60 MINUTE'
AS
INSERT INTO summary_table
SELECT DATE_TRUNC('hour', timestamp), COUNT(*)
FROM events
WHERE timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY 1;

-- Task with condition
CREATE TASK process_orders
    WAREHOUSE = my_warehouse
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
INSERT INTO processed_orders
SELECT * FROM orders_stream;

-- Serverless task (no warehouse needed)
CREATE TASK serverless_task
    SCHEDULE = '10 MINUTE'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
INSERT INTO logs SELECT * FROM staging_logs;
```

#### 11.2 Task Dependencies (DAGs)
```sql
-- Root task
CREATE TASK root_task
    WAREHOUSE = my_warehouse
    SCHEDULE = '60 MINUTE'
AS
INSERT INTO stage1 SELECT * FROM source;

-- Child task (depends on root_task)
CREATE TASK child_task_1
    WAREHOUSE = my_warehouse
    AFTER root_task
AS
INSERT INTO stage2 SELECT * FROM stage1;

-- Another child task
CREATE TASK child_task_2
    WAREHOUSE = my_warehouse
    AFTER root_task
AS
INSERT INTO stage3 SELECT * FROM stage1;

-- Grandchild task
CREATE TASK final_task
    WAREHOUSE = my_warehouse
    AFTER child_task_1, child_task_2
AS
INSERT INTO final_table 
SELECT * FROM stage2 
UNION ALL 
SELECT * FROM stage3;
```

#### 11.3 Managing Tasks
```sql
-- Resume/suspend tasks
ALTER TASK my_task RESUME;
ALTER TASK my_task SUSPEND;

-- Modify task
ALTER TASK my_task SET WAREHOUSE = larger_warehouse;
ALTER TASK my_task SET SCHEDULE = '30 MINUTE';

-- Execute manually
EXECUTE TASK my_task;

-- Show tasks
SHOW TASKS;
DESC TASK my_task;

-- Task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'MY_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP())
));

-- Drop task
DROP TASK IF EXISTS my_task;
```

### 12. Stored Procedures

#### 12.1 SQL Stored Procedures
```sql
-- Basic stored procedure
CREATE PROCEDURE cleanup_old_data(days_to_keep INTEGER)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM events WHERE event_date < DATEADD(day, -:days_to_keep, CURRENT_DATE());
    RETURN 'Cleanup completed';
END;
$$;

-- Calling procedures
CALL cleanup_old_data(30);

-- Procedure with cursor
CREATE PROCEDURE process_customers()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    c1 CURSOR FOR SELECT customer_id, email FROM customers;
    cust_id INTEGER;
    cust_email VARCHAR;
BEGIN
    OPEN c1;
    FETCH c1 INTO cust_id, cust_email;
    WHILE (SQLCODE = 0) DO
        -- Process each customer
        INSERT INTO customer_log VALUES (:cust_id, :cust_email, CURRENT_TIMESTAMP());
        FETCH c1 INTO cust_id, cust_email;
    END WHILE;
    CLOSE c1;
    RETURN 'Processing completed';
END;
$$;

-- Procedure with error handling
CREATE PROCEDURE safe_delete(table_name VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM IDENTIFIER(:table_name) WHERE created_date < '2020-01-01';
    RETURN 'Success';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error: ' || SQLERRM;
END;
$$;
```

#### 12.2 JavaScript Stored Procedures
```sql
-- JavaScript procedure
CREATE PROCEDURE process_json_data(json_string VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var data = JSON.parse(JSON_STRING);
    var result = {};
    result.count = data.length;
    result.first = data[0];
    return result;
$$;

-- JavaScript with SQL execution
CREATE PROCEDURE dynamic_insert(table_name VARCHAR, value VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var sql_command = `INSERT INTO IDENTIFIER(?) VALUES (?)`;
    snowflake.execute({
        sqlText: sql_command,
        binds: [TABLE_NAME, VALUE]
    });
    return 'Insert completed';
$$;

-- JavaScript with result set processing
CREATE PROCEDURE aggregate_by_group()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var result = {};
    var stmt = snowflake.createStatement({
        sqlText: `SELECT country, COUNT(*) as cnt FROM customers GROUP BY country`
    });
    var rs = stmt.execute();
    while (rs.next()) {
        result[rs.getColumnValue(1)] = rs.getColumnValue(2);
    }
    return result;
$$;
```

### 13. User-Defined Functions (UDFs)

#### 13.1 SQL UDFs
```sql
-- Scalar SQL UDF
CREATE FUNCTION calculate_discount(price FLOAT, discount_pct FLOAT)
RETURNS FLOAT
AS
$$
    price * (1 - discount_pct / 100)
$$;

-- Using UDF
SELECT product_name, price, calculate_discount(price, 10) AS discounted_price
FROM products;

-- Secure UDF (hides definition)
CREATE SECURE FUNCTION mask_email(email VARCHAR)
RETURNS VARCHAR
AS
$$
    REGEXP_REPLACE(email, '^(.{2}).*(@.*)', '\\1***\\2')
$$;
```

#### 13.2 JavaScript UDFs
```sql
-- JavaScript scalar UDF
CREATE FUNCTION calculate_tax(amount FLOAT, tax_rate FLOAT)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    return AMOUNT * (1 + TAX_RATE);
$$;

-- JavaScript UDF with complex logic
CREATE FUNCTION parse_user_agent(ua VARCHAR)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS
$$
    var result = {};
    if (UA.indexOf('Chrome') > -1) {
        result.browser = 'Chrome';
    } else if (UA.indexOf('Firefox') > -1) {
        result.browser = 'Firefox';
    } else {
        result.browser = 'Other';
    }
    return result;
$$;
```

#### 13.3 Table Functions (UDTFs)
```sql
-- SQL table function
CREATE FUNCTION split_string(input_string VARCHAR)
RETURNS TABLE (part VARCHAR)
AS
$$
    SELECT VALUE AS part
    FROM TABLE(SPLIT_TO_TABLE(input_string, ','))
$$;

-- Using table function
SELECT * FROM TABLE(split_string('apple,banana,orange'));

-- JavaScript table function
CREATE FUNCTION generate_sequence(start_num INTEGER, end_num INTEGER)
RETURNS TABLE (num INTEGER)
LANGUAGE JAVASCRIPT
AS
$$
    {
        processRow: function(row, rowWriter, context) {
            for (var i = row.START_NUM; i <= row.END_NUM; i++) {
                rowWriter.writeRow({NUM: i});
            }
        }
    }
$$;
```

### 14. Materialized Views

#### 14.1 Creating Materialized Views
```sql
-- Basic materialized view
CREATE MATERIALIZED VIEW customer_summary_mv AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Secure materialized view
CREATE SECURE MATERIALIZED VIEW secure_summary_mv AS
SELECT 
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id;

-- Clustering materialized view
CREATE MATERIALIZED VIEW clustered_mv
CLUSTER BY (order_date)
AS
SELECT * FROM large_orders_table;
```

#### 14.2 Managing Materialized Views
```sql
-- Manually refresh (automatic by default)
ALTER MATERIALIZED VIEW customer_summary_mv SUSPEND;
ALTER MATERIALIZED VIEW customer_summary_mv RESUME;

-- Show materialized views
SHOW MATERIALIZED VIEWS;

-- Check if behind (needs refresh)
SELECT * FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY(
    VIEW_NAME => 'CUSTOMER_SUMMARY_MV'
));

-- Drop materialized view
DROP MATERIALIZED VIEW customer_summary_mv;
```

### 15. Snowpipe (Continuous Loading)

#### 15.1 Creating Snowpipes
```sql
-- Basic Snowpipe
CREATE PIPE my_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO my_table
FROM @my_s3_stage
FILE_FORMAT = (TYPE = 'JSON');

-- Snowpipe with error handling
CREATE PIPE orders_pipe
    AUTO_INGEST = TRUE
    ERROR_INTEGRATION = my_notification_integration
AS
COPY INTO orders
FROM @orders_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
ON_ERROR = 'SKIP_FILE';

-- Showing pipes
SHOW PIPES;
DESC PIPE my_pipe;
```

#### 15.2 Managing Snowpipes
```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('my_pipe');

-- Manually refresh pipe
ALTER PIPE my_pipe REFRESH;

-- Pause/resume pipe
ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = FALSE;

-- View pipe history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP()),
    PIPE_NAME => 'MY_PIPE'
));

-- Copy history for Snowpipe
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MY_TABLE',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));
```

#### 15.3 Snowpipe with Cloud Storage Events
```sql
-- AWS S3 Event Notification
-- 1. Get SQS queue ARN from pipe
DESC PIPE my_pipe;  -- Look for notification_channel

-- 2. Configure S3 bucket event notification to send to SQS

-- Azure Event Grid
-- Similar setup with Azure Blob Storage and Event Grid

-- GCS Pub/Sub
-- Similar setup with GCS and Pub/Sub
```

---

## ADVANCED LEVEL

### 16. Security and Access Control

#### 16.1 Users and Roles
```sql
-- Creating users
CREATE USER john_doe 
    PASSWORD = 'StrongPassword123!'
    DEFAULT_ROLE = analyst
    DEFAULT_WAREHOUSE = compute_wh
    MUST_CHANGE_PASSWORD = TRUE;

-- Modifying users
ALTER USER john_doe SET PASSWORD = 'NewPassword456!';
ALTER USER john_doe SET DEFAULT_ROLE = developer;
ALTER USER john_doe RESET PASSWORD;

-- Creating roles
CREATE ROLE data_analyst;
CREATE ROLE data_engineer;
CREATE ROLE data_scientist;

-- Role hierarchy
GRANT ROLE data_analyst TO ROLE data_scientist;
GRANT ROLE data_engineer TO USER john_doe;

-- Showing users and roles
SHOW USERS;
SHOW ROLES;
SHOW GRANTS TO USER john_doe;
SHOW GRANTS TO ROLE data_analyst;
```

#### 16.2 Privileges and Grants
```sql
-- Database privileges
GRANT USAGE ON DATABASE my_database TO ROLE data_analyst;
GRANT CREATE SCHEMA ON DATABASE my_database TO ROLE data_engineer;
GRANT OWNERSHIP ON DATABASE my_database TO ROLE admin;

-- Schema privileges
GRANT USAGE ON SCHEMA my_schema TO ROLE data_analyst;
GRANT CREATE TABLE ON SCHEMA my_schema TO ROLE data_engineer;
GRANT ALL PRIVILEGES ON SCHEMA my_schema TO ROLE admin;

-- Table privileges
GRANT SELECT ON TABLE customers TO ROLE data_analyst;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE orders TO ROLE data_engineer;
GRANT ALL PRIVILEGES ON TABLE sensitive_data TO ROLE admin;

-- Warehouse privileges
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE data_analyst;
GRANT OPERATE ON WAREHOUSE compute_wh TO ROLE data_engineer;
GRANT ALL PRIVILEGES ON WAREHOUSE compute_wh TO ROLE admin;

-- Future grants (apply to objects created in future)
GRANT SELECT ON FUTURE TABLES IN SCHEMA my_schema TO ROLE data_analyst;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE my_database TO ROLE data_analyst;

-- Revoking privileges
REVOKE SELECT ON TABLE customers FROM ROLE data_analyst;
REVOKE USAGE ON WAREHOUSE compute_wh FROM ROLE data_analyst;
```

#### 16.3 Row Access Policies
```sql
-- Creating row access policy
CREATE ROW ACCESS POLICY region_policy AS (region_column VARCHAR) 
RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() = 'ADMIN' THEN TRUE
        WHEN CURRENT_ROLE() = 'US_ANALYST' AND region_column = 'US' THEN TRUE
        WHEN CURRENT_ROLE() = 'EU_ANALYST' AND region_column = 'EU' THEN TRUE
        ELSE FALSE
    END;

-- Applying row access policy
ALTER TABLE sales ADD ROW ACCESS POLICY region_policy ON (region);

-- Multiple policies (all must be true)
ALTER TABLE sales ADD ROW ACCESS POLICY date_policy ON (sale_date);

-- Dropping row access policy
ALTER TABLE sales DROP ROW ACCESS POLICY region_policy;
DROP ROW ACCESS POLICY region_policy;

-- Showing policies
SHOW ROW ACCESS POLICIES;
DESC ROW ACCESS POLICY region_policy;
```

#### 16.4 Column Masking Policies
```sql
-- Creating masking policy
CREATE MASKING POLICY email_mask AS (val VARCHAR) 
RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('ADMIN', 'PRIVACY_OFFICER') THEN val
        ELSE REGEXP_REPLACE(val, '^(.{2}).*(@.*)', '\\1***\\2')
    END;

-- Applying masking policy
ALTER TABLE customers MODIFY COLUMN email 
SET MASKING POLICY email_mask;

-- SSN masking
CREATE MASKING POLICY ssn_mask AS (val VARCHAR) 
RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() = 'ADMIN' THEN val
        ELSE '***-**-' || RIGHT(val, 4)
    END;

-- Conditional masking
CREATE MASKING POLICY salary_mask AS (val NUMBER, job_role VARCHAR) 
RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('ADMIN', 'HR') THEN val
        WHEN CURRENT_ROLE() = 'MANAGER' AND job_role = 'ANALYST' THEN val
        ELSE NULL
    END;

-- Unset masking policy
ALTER TABLE customers MODIFY COLUMN email 
UNSET MASKING POLICY;

-- Drop masking policy
DROP MASKING POLICY email_mask;
```

#### 16.5 Network Policies
```sql
-- Creating network policy
CREATE NETWORK POLICY office_only
    ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8')
    BLOCKED_IP_LIST = ('192.168.1.100');

-- Applying to account
ALTER ACCOUNT SET NETWORK_POLICY = office_only;

-- Applying to user
ALTER USER john_doe SET NETWORK_POLICY = office_only;

-- Showing network policies
SHOW NETWORK POLICIES;
DESC NETWORK POLICY office_only;

-- Dropping network policy
ALTER ACCOUNT UNSET NETWORK_POLICY;
DROP NETWORK POLICY office_only;
```

### 17. Data Sharing

#### 17.1 Creating Shares (as Provider)
```sql
-- Create share
CREATE SHARE customer_data_share;

-- Add database to share
GRANT USAGE ON DATABASE sales_db TO SHARE customer_data_share;
GRANT USAGE ON SCHEMA sales_db.public TO SHARE customer_data_share;

-- Add specific tables
GRANT SELECT ON TABLE sales_db.public.orders TO SHARE customer_data_share;
GRANT SELECT ON TABLE sales_db.public.customers TO SHARE customer_data_share;

-- Share secure views instead of tables (recommended)
CREATE SECURE VIEW shared_orders AS
SELECT order_id, customer_id, amount, order_date
FROM orders
WHERE order_date >= DATEADD(year, -1, CURRENT_DATE());

GRANT SELECT ON VIEW sales_db.public.shared_orders TO SHARE customer_data_share;

-- Add consumer accounts
ALTER SHARE customer_data_share ADD ACCOUNTS = xy12345, ab67890;

-- Show shares
SHOW SHARES;
DESC SHARE customer_data_share;
```

#### 17.2 Consuming Shares (as Consumer)
```sql
-- Show available shares
SHOW SHARES;

-- Create database from share
CREATE DATABASE shared_sales_data 
FROM SHARE provider_account.customer_data_share;

-- Query shared data
SELECT * FROM shared_sales_data.public.orders;

-- Create views on shared data
CREATE VIEW local_orders AS
SELECT * FROM shared_sales_data.public.orders
WHERE region = 'US';
```

#### 17.3 Secure Data Sharing Best Practices
- Always use SECURE views for sharing
- Apply row access policies before sharing
- Use masking policies to protect PII
- Regularly audit what's being shared
- Monitor consumer usage
- Version shared views for changes

### 18. Resource Monitors

#### 18.1 Creating Resource Monitors
```sql
-- Account-level resource monitor
CREATE RESOURCE MONITOR account_monitor WITH
    CREDIT_QUOTA = 1000
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Warehouse-specific resource monitor
CREATE RESOURCE MONITOR warehouse_monitor WITH
    CREDIT_QUOTA = 100
    FREQUENCY = WEEKLY
    START_TIMESTAMP = '2024-01-01 00:00 UTC'
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Applying to account
ALTER ACCOUNT SET RESOURCE_MONITOR = account_monitor;

-- Applying to warehouse
ALTER WAREHOUSE compute_wh SET RESOURCE_MONITOR = warehouse_monitor;

-- Showing resource monitors
SHOW RESOURCE MONITORS;

-- Modifying resource monitor
ALTER RESOURCE MONITOR account_monitor SET CREDIT_QUOTA = 1500;

-- Dropping resource monitor
DROP RESOURCE MONITOR warehouse_monitor;
```

### 19. Query Optimization

#### 19.1 Understanding Query Profile
```sql
-- Enable query profiling in session
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Run query and analyze profile in Snowsight
SELECT * FROM large_table WHERE date_col = '2024-01-01';

-- Key metrics to look for:
-- - Bytes scanned vs bytes sent
-- - Partitions scanned vs total partitions
-- - Spillage to local/remote disk
-- - Join types and efficiency
```

#### 19.2 Clustering Keys
```sql
-- Add clustering key to existing table
ALTER TABLE large_table CLUSTER BY (date_col, category);

-- Create table with clustering
CREATE TABLE events (
    event_id INTEGER,
    event_date DATE,
    user_id INTEGER,
    event_type VARCHAR
) CLUSTER BY (event_date, event_type);

-- Check clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('large_table', '(date_col, category)');

-- Reclustering (automatic in background)
-- Monitor clustering depth
SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    TABLE_NAME => 'LARGE_TABLE'
));

-- Suspend/resume automatic clustering
ALTER TABLE large_table SUSPEND RECLUSTER;
ALTER TABLE large_table RESUME RECLUSTER;

-- Drop clustering keys
ALTER TABLE large_table DROP CLUSTERING KEY;
```

#### 19.3 Search Optimization Service
```sql
-- Enable search optimization
ALTER TABLE large_table ADD SEARCH OPTIMIZATION;

-- Optimize specific columns
ALTER TABLE large_table ADD SEARCH OPTIMIZATION ON EQUALITY(category, status);

-- Check search optimization status
DESC SEARCH OPTIMIZATION ON large_table;

-- Show search optimization history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
    TABLE_NAME => 'LARGE_TABLE'
));

-- Drop search optimization
ALTER TABLE large_table DROP SEARCH OPTIMIZATION;
```

#### 19.4 Result Caching
```sql
-- Enable result caching (default)
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Disable for specific query
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
SELECT * FROM large_table;

-- Result cache is used when:
-- - Exact same query
-- - No changes to underlying data
-- - Within 24-hour window
-- - User has access to data
```

#### 19.5 Query Optimization Techniques
```sql
-- Use LIMIT for testing
SELECT * FROM large_table LIMIT 1000;

-- Select only needed columns
SELECT col1, col2 FROM large_table;  -- Better
SELECT * FROM large_table;  -- Avoid

-- Use WHERE to prune partitions
SELECT * FROM large_table 
WHERE date_col = '2024-01-01';  -- Prunes partitions

-- Avoid functions on indexed/clustered columns
-- Bad
SELECT * FROM large_table WHERE YEAR(date_col) = 2024;
-- Good
SELECT * FROM large_table 
WHERE date_col BETWEEN '2024-01-01' AND '2024-12-31';

-- Use QUALIFY for window functions
SELECT customer_id, order_id, amount,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
FROM orders
QUALIFY rn = 1;  -- More efficient than subquery

-- Avoid DISTINCT when possible
SELECT customer_id FROM orders GROUP BY customer_id;  -- Better
SELECT DISTINCT customer_id FROM orders;  -- Slower

-- Use CTEs for readability and optimization
WITH filtered_data AS (
    SELECT * FROM large_table WHERE date_col >= '2024-01-01'
)
SELECT * FROM filtered_data WHERE category = 'A';
```

### 20. External Tables

#### 20.1 Creating External Tables
```sql
-- S3 external table with partitions
CREATE EXTERNAL TABLE ext_orders (
    order_id INTEGER AS (value:order_id::INTEGER),
    customer_id INTEGER AS (value:customer_id::INTEGER),
    amount FLOAT AS (value:amount::FLOAT),
    order_date DATE AS (value:order_date::DATE)
)
PARTITION BY (order_date)
LOCATION = @my_s3_stage/orders/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE;

-- Azure external table
CREATE EXTERNAL TABLE ext_customers
WITH LOCATION = @my_azure_stage/customers/
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*customers.*\\.json';

-- Partitioned external table
CREATE EXTERNAL TABLE ext_events (
    event_id INTEGER AS (value:event_id::INTEGER),
    event_type VARCHAR AS (value:event_type::VARCHAR),
    year VARCHAR AS (SUBSTR(METADATA$FILENAME, 1, 4)),
    month VARCHAR AS (SUBSTR(METADATA$FILENAME, 6, 2))
)
PARTITION BY (year, month)
LOCATION = @my_s3_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
AUTO_REFRESH = TRUE
REFRESH_ON_CREATE = TRUE;
```

#### 20.2 Managing External Tables
```sql
-- Manually refresh external table metadata
ALTER EXTERNAL TABLE ext_orders REFRESH;

-- Add partition manually
ALTER EXTERNAL TABLE ext_orders 
ADD PARTITION(order_date='2024-01-01') 
LOCATION 'orders/date=2024-01-01/';

-- Drop partition
ALTER EXTERNAL TABLE ext_orders 
DROP PARTITION(order_date='2024-01-01');

-- Show external tables
SHOW EXTERNAL TABLES;

-- Querying external tables
SELECT * FROM ext_orders WHERE order_date = '2024-01-01';

-- Metadata columns
SELECT 
    *,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER
FROM ext_orders;
```

### 21. Data Governance with Tags

#### 21.1 Creating and Using Tags
```sql
-- Create tag
CREATE TAG pii_level 
    ALLOWED_VALUES 'high', 'medium', 'low', 'none';

CREATE TAG data_classification
    ALLOWED_VALUES 'public', 'internal', 'confidential', 'restricted';

-- Apply tags to objects
ALTER TABLE customers SET TAG pii_level = 'high';
ALTER TABLE customers SET TAG data_classification = 'confidential';

-- Apply tags to columns
ALTER TABLE customers MODIFY COLUMN email 
    SET TAG pii_level = 'high';

ALTER TABLE customers MODIFY COLUMN phone 
    SET TAG pii_level = 'medium';

-- Apply tags to database/schema
ALTER DATABASE sales_db SET TAG data_classification = 'internal';
ALTER SCHEMA sales_db.public SET TAG data_classification = 'public';

-- Show tags
SHOW TAGS;
DESC TAG pii_level;

-- Query tag references
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE TAG_NAME = 'PII_LEVEL';

-- Use tags in masking policies
CREATE MASKING POLICY tag_based_mask AS (val VARCHAR) 
RETURNS VARCHAR ->
    CASE
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('PII_LEVEL') = 'high'
             AND CURRENT_ROLE() NOT IN ('ADMIN', 'PRIVACY_OFFICER')
        THEN '***REDACTED***'
        ELSE val
    END;

-- Unset tags
ALTER TABLE customers UNSET TAG pii_level;

-- Drop tag
DROP TAG pii_level;
```

### 22. Replication and Failover

#### 22.1 Database Replication
```sql
-- Enable replication on primary account
ALTER DATABASE sales_db ENABLE REPLICATION TO ACCOUNTS xy12345.us-east-1;

-- On secondary account, create replica
CREATE DATABASE sales_db_replica 
AS REPLICA OF xy67890.us-west-2.sales_db;

-- Refresh replica (manual or scheduled)
ALTER DATABASE sales_db_replica REFRESH;

-- Show replication databases
SHOW REPLICATION DATABASES;

-- Show replication accounts
SHOW REPLICATION ACCOUNTS;

-- Monitoring replication lag
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
WHERE DATABASE_NAME = 'SALES_DB'
ORDER BY START_TIME DESC;
```

#### 22.2 Failover and Failback
```sql
-- Promote secondary to primary (failover)
ALTER DATABASE sales_db_replica PRIMARY;

-- On original primary, create replica of new primary
CREATE DATABASE sales_db_replica
AS REPLICA OF xy12345.us-east-1.sales_db;

-- Failback (when original primary is ready)
ALTER DATABASE sales_db PRIMARY;
```

### 23. Account Usage and Information Schema

#### 23.1 Account Usage Views
```sql
-- Query history (up to 1 year)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 100;

-- Warehouse metering history
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(month, -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- Storage usage
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE USAGE_DATE >= DATEADD(month, -1, CURRENT_TIMESTAMP())
ORDER BY USAGE_DATE DESC;

-- Table storage metrics
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = 'MY_DATABASE'
ORDER BY ACTIVE_BYTES DESC;

-- Access history (Enterprise+)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE QUERY_START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP());

-- Login history
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY EVENT_TIMESTAMP DESC;

-- Users and roles
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.USERS;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES;
```

#### 23.2 Information Schema
```sql
-- Current session information
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC';
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'CUSTOMERS';
SELECT * FROM INFORMATION_SCHEMA.VIEWS;
SELECT * FROM INFORMATION_SCHEMA.SCHEMATA;

-- Table functions for recent history
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    RESULT_LIMIT => 100
));

SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(
    WAREHOUSE_NAME => 'COMPUTE_WH',
    START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())
));

SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'MY_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP())
));
```

### 24. Performance Monitoring and Tuning

#### 24.1 Identifying Expensive Queries
```sql
-- Top 10 longest running queries
SELECT 
    query_id,
    query_text,
    user_name,
    warehouse_name,
    TOTAL_ELAPSED_TIME/1000 AS duration_seconds,
    BYTES_SCANNED,
    ROWS_PRODUCED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 10;

-- Queries with high spillage
SELECT 
    query_id,
    query_text,
    user_name,
    warehouse_name,
    BYTES_SPILLED_TO_LOCAL_STORAGE,
    BYTES_SPILLED_TO_REMOTE_STORAGE
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0 
       OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
ORDER BY BYTES_SPILLED_TO_REMOTE_STORAGE DESC;

-- Queries scanning the most data
SELECT 
    query_id,
    query_text,
    user_name,
    BYTES_SCANNED,
    BYTES_SCANNED / NULLIF(ROWS_PRODUCED, 0) AS bytes_per_row
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY BYTES_SCANNED DESC
LIMIT 20;
```

#### 24.2 Warehouse Utilization
```sql
-- Warehouse credit usage
SELECT 
    warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used_compute) AS compute_credits,
    SUM(credits_used_cloud_services) AS cloud_services_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(month, -1, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- Warehouse queuing
SELECT 
    warehouse_name,
    AVG(AVG_QUEUED_LOAD) AS avg_queued_load,
    MAX(AVG_QUEUED_LOAD) AS max_queued_load
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
HAVING AVG(AVG_QUEUED_LOAD) > 0;
```

---

## EXPERT LEVEL

### 25. Advanced Architectures

#### 25.1 Multi-Tenant Architecture Patterns
```sql
-- Pattern 1: Schema per tenant
CREATE DATABASE saas_app;
CREATE SCHEMA saas_app.tenant_001;
CREATE SCHEMA saas_app.tenant_002;

-- Pattern 2: Database per tenant (better isolation)
CREATE DATABASE tenant_001;
CREATE DATABASE tenant_002;

-- Pattern 3: Shared tables with tenant_id
CREATE TABLE shared_data (
    tenant_id VARCHAR(50),
    data_id INTEGER,
    data_value VARCHAR,
    PRIMARY KEY (tenant_id, data_id)
) CLUSTER BY (tenant_id);

-- Row access policy for multi-tenancy
CREATE ROW ACCESS POLICY tenant_isolation AS (tenant_id VARCHAR) 
RETURNS BOOLEAN ->
    tenant_id = CURRENT_USER()  -- Assuming username = tenant_id
    OR CURRENT_ROLE() = 'ADMIN';

ALTER TABLE shared_data 
ADD ROW ACCESS POLICY tenant_isolation ON (tenant_id);
```

#### 25.2 Data Lake Architecture
```sql
-- Bronze layer (raw data)
CREATE DATABASE bronze_layer;
CREATE SCHEMA bronze_layer.raw;

CREATE TABLE bronze_layer.raw.events (
    raw_data VARIANT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR
);

-- Silver layer (cleansed data)
CREATE DATABASE silver_layer;
CREATE SCHEMA silver_layer.clean;

CREATE TABLE silver_layer.clean.events (
    event_id VARCHAR,
    event_type VARCHAR,
    user_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    properties VARIANT,
    processing_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Gold layer (aggregated/business data)
CREATE DATABASE gold_layer;
CREATE SCHEMA gold_layer.analytics;

CREATE TABLE gold_layer.analytics.daily_user_activity (
    activity_date DATE,
    user_id VARCHAR,
    event_count INTEGER,
    unique_event_types INTEGER,
    first_event_timestamp TIMESTAMP_NTZ,
    last_event_timestamp TIMESTAMP_NTZ
);
```

#### 25.3 Slowly Changing Dimensions (SCD)
```sql
-- SCD Type 2 implementation
CREATE TABLE dim_customer (
    customer_key INTEGER AUTOINCREMENT,
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN,
    PRIMARY KEY (customer_key)
);

-- SCD Type 2 merge pattern
MERGE INTO dim_customer t
USING (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        CURRENT_DATE() AS effective_date
    FROM staging_customers
) s
ON t.customer_id = s.customer_id AND t.is_current = TRUE
WHEN MATCHED AND (
    t.first_name != s.first_name OR 
    t.last_name != s.last_name OR 
    t.email != s.email
) THEN UPDATE SET
    t.is_current = FALSE,
    t.expiration_date = CURRENT_DATE() - 1
WHEN NOT MATCHED THEN INSERT (
    customer_id, first_name, last_name, email, 
    effective_date, expiration_date, is_current
) VALUES (
    s.customer_id, s.first_name, s.last_name, s.email,
    s.effective_date, '9999-12-31', TRUE
);

-- Insert new versions for changed records
INSERT INTO dim_customer (customer_id, first_name, last_name, email, effective_date, expiration_date, is_current)
SELECT 
    s.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    CURRENT_DATE(),
    '9999-12-31',
    TRUE
FROM staging_customers s
JOIN dim_customer t ON s.customer_id = t.customer_id
WHERE t.is_current = FALSE 
  AND t.expiration_date = CURRENT_DATE() - 1;
```

### 26. Advanced Data Engineering Patterns

#### 26.1 Incremental Processing with Streams
```sql
-- Create stream on source table
CREATE STREAM orders_stream ON TABLE orders;

-- Incremental load pattern
CREATE OR REPLACE PROCEDURE process_orders_incrementally()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Check if stream has data
    IF (SYSTEM$STREAM_HAS_DATA('orders_stream')) THEN
        -- Process inserts
        INSERT INTO orders_processed
        SELECT * FROM orders_stream
        WHERE METADATA$ACTION = 'INSERT';
        
        -- Process updates
        MERGE INTO orders_processed t
        USING (
            SELECT * FROM orders_stream 
            WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE
        ) s ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET
            t.status = s.status,
            t.updated_at = s.updated_at;
        
        -- Process deletes
        DELETE FROM orders_processed
        WHERE order_id IN (
            SELECT order_id FROM orders_stream 
            WHERE METADATA$ACTION = 'DELETE'
        );
        
        RETURN 'Processing completed';
    ELSE
        RETURN 'No new data to process';
    END IF;
END;
$$;

-- Schedule task
CREATE TASK process_orders_task
    WAREHOUSE = etl_warehouse
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
CALL process_orders_incrementally();
```

#### 26.2 Complex Task DAG
```sql
-- Level 1: Extract tasks
CREATE TASK extract_orders
    WAREHOUSE = etl_warehouse
    SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
COPY INTO staging_orders FROM @orders_stage;

CREATE TASK extract_customers
    WAREHOUSE = etl_warehouse
    SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
COPY INTO staging_customers FROM @customers_stage;

-- Level 2: Transform tasks (depend on extract)
CREATE TASK transform_orders
    WAREHOUSE = etl_warehouse
    AFTER extract_orders
AS
INSERT INTO clean_orders
SELECT 
    order_id,
    customer_id,
    TO_DATE(order_date, 'YYYY-MM-DD') AS order_date,
    amount
FROM staging_orders;

CREATE TASK transform_customers
    WAREHOUSE = etl_warehouse
    AFTER extract_customers
AS
INSERT INTO clean_customers
SELECT 
    customer_id,
    UPPER(first_name) AS first_name,
    UPPER(last_name) AS last_name,
    LOWER(email) AS email
FROM staging_customers;

-- Level 3: Aggregate task (depends on both transforms)
CREATE TASK aggregate_sales
    WAREHOUSE = etl_warehouse
    AFTER transform_orders, transform_customers
AS
INSERT INTO daily_sales_summary
SELECT 
    o.order_date,
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.amount) AS total_amount
FROM clean_orders o
JOIN clean_customers c ON o.customer_id = c.customer_id
GROUP BY o.order_date, c.customer_id, c.first_name, c.last_name;

-- Resume all tasks (in correct order)
ALTER TASK aggregate_sales RESUME;
ALTER TASK transform_orders RESUME;
ALTER TASK transform_customers RESUME;
ALTER TASK extract_orders RESUME;
ALTER TASK extract_customers RESUME;
```

#### 26.3 Dynamic SQL and Metadata-Driven Pipelines
```sql
-- Metadata table
CREATE TABLE etl_metadata (
    table_name VARCHAR,
    source_location VARCHAR,
    target_schema VARCHAR,
    file_format VARCHAR,
    load_type VARCHAR,  -- FULL, INCREMENTAL
    last_load_timestamp TIMESTAMP_NTZ
);

-- Dynamic ETL procedure
CREATE OR REPLACE PROCEDURE dynamic_load()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var result_array = [];
    
    // Get tables to process
    var stmt = snowflake.createStatement({
        sqlText: `SELECT table_name, source_location, target_schema, file_format 
                  FROM etl_metadata 
                  WHERE load_type = 'FULL'`
    });
    var rs = stmt.execute();
    
    while (rs.next()) {
        var table_name = rs.getColumnValue(1);
        var source_location = rs.getColumnValue(2);
        var target_schema = rs.getColumnValue(3);
        var file_format = rs.getColumnValue(4);
        
        try {
            // Truncate target table
            var truncate_stmt = snowflake.createStatement({
                sqlText: `TRUNCATE TABLE ${target_schema}.${table_name}`
            });
            truncate_stmt.execute();
            
            // Load data
            var copy_stmt = snowflake.createStatement({
                sqlText: `COPY INTO ${target_schema}.${table_name} 
                          FROM ${source_location} 
                          FILE_FORMAT = (FORMAT_NAME = '${file_format}')`
            });
            copy_stmt.execute();
            
            // Update metadata
            var update_stmt = snowflake.createStatement({
                sqlText: `UPDATE etl_metadata 
                          SET last_load_timestamp = CURRENT_TIMESTAMP() 
                          WHERE table_name = '${table_name}'`
            });
            update_stmt.execute();
            
            result_array.push(`Success: ${table_name}`);
        } catch (err) {
            result_array.push(`Error loading ${table_name}: ${err.message}`);
        }
    }
    
    return result_array.join('\n');
$$;
```

### 27. Advanced Security Patterns

#### 27.1 Attribute-Based Access Control (ABAC)
```sql
-- User attributes table
CREATE TABLE user_attributes (
    user_name VARCHAR,
    department VARCHAR,
    region VARCHAR,
    clearance_level VARCHAR
);

-- Session variables for attributes
CREATE OR REPLACE PROCEDURE set_user_context()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    LET dept VARCHAR;
    LET region VARCHAR;
    SELECT department, region INTO :dept, :region
    FROM user_attributes
    WHERE user_name = CURRENT_USER();
    
    EXECUTE IMMEDIATE 'ALTER SESSION SET user_department = ' || :dept;
    EXECUTE IMMEDIATE 'ALTER SESSION SET user_region = ' || :region;
    RETURN 'Context set';
END;
$$;

-- Row access policy using attributes
CREATE ROW ACCESS POLICY attribute_based_access AS (dept VARCHAR, region VARCHAR)
RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() = 'ADMIN' THEN TRUE
        WHEN dept = CURRENT_SESSION_PARAMETER('user_department') 
             AND region = CURRENT_SESSION_PARAMETER('user_region') THEN TRUE
        ELSE FALSE
    END;
```

#### 27.2 Encryption and Key Management
```sql
-- Client-side encryption (use external key management)
-- Snowflake handles encryption at rest automatically

-- Column-level encryption (application level)
CREATE TABLE encrypted_data (
    id INTEGER,
    sensitive_data VARCHAR,  -- Store encrypted
    encrypted_key VARCHAR    -- Store encrypted key
);

-- Using Tri-Secret Secure (Business Critical+)
-- Requires configuration at account level with customer-managed key

-- Rekeying database
ALTER DATABASE my_database ENABLE CHANGE_TRACKING;
```

### 28. Advanced Optimization Techniques

#### 28.1 Query Acceleration Service
```sql
-- Enable query acceleration for warehouse
ALTER WAREHOUSE compute_wh SET ENABLE_QUERY_ACCELERATION = TRUE;
ALTER WAREHOUSE compute_wh SET QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;

-- Check if query used acceleration
SELECT 
    query_id,
    eligible_query_acceleration_time,
    upper_limit_scale_factor,
    query_acceleration_bytes_scanned,
    query_acceleration_partitions_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP());
```

#### 28.2 Partition Pruning Optimization
```sql
-- Analyze partition pruning
SELECT 
    query_id,
    query_text,
    partitions_scanned,
    partitions_total,
    bytes_scanned,
    (partitions_scanned::FLOAT / NULLIF(partitions_total, 0)) * 100 AS pct_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE partitions_total > 0
  AND START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY pct_scanned DESC;

-- Optimize with better clustering
-- Bad: causes full table scan
SELECT * FROM events WHERE YEAR(event_date) = 2024;

-- Good: prunes partitions effectively
SELECT * FROM events 
WHERE event_date BETWEEN '2024-01-01' AND '2024-12-31';
```

### 29. Machine Learning with Snowpark

#### 29.1 Snowpark Python for ML
```python
# Snowpark DataFrame operations
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum, avg

session = Session.builder.configs(connection_parameters).create()

# Load data
df = session.table("sales_data")

# Transformations
result = df.filter(col("amount") > 100) \
           .group_by("customer_id") \
           .agg(sum("amount").alias("total_spent"),
                avg("amount").alias("avg_order_value"))

# Write results
result.write.mode("overwrite").save_as_table("customer_metrics")

# Machine learning example
from snowflake.ml.modeling.preprocessing import StandardScaler
from snowflake.ml.modeling.linear_model import LogisticRegression

# Feature engineering
features = ["age", "income", "credit_score"]
scaler = StandardScaler(input_cols=features, output_cols=["scaled_" + f for f in features])
scaled_df = scaler.fit(df).transform(df)

# Train model
lr = LogisticRegression(input_cols=["scaled_age", "scaled_income", "scaled_credit_score"],
                       label_cols=["churned"])
model = lr.fit(scaled_df)

# Predictions
predictions = model.predict(test_df)
```

#### 29.2 Storing and Using ML Models
```sql
-- Store model artifacts in stage
PUT file:///local/path/model.pkl @ml_models/churn_model/;

-- Create UDF for model inference
CREATE OR REPLACE FUNCTION predict_churn(age FLOAT, income FLOAT, credit_score FLOAT)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('scikit-learn', 'pandas', 'joblib')
IMPORTS = ('@ml_models/churn_model/model.pkl')
HANDLER = 'predict'
AS
$$
import joblib
import pandas as pd

def predict(age, income, credit_score):
    import_dir = import_directory('model.pkl')
    model = joblib.load(import_dir + 'model.pkl')
    features = pd.DataFrame([[age, income, credit_score]], 
                           columns=['age', 'income', 'credit_score'])
    return model.predict_proba(features)[0][1]
$$;

-- Use in queries
SELECT 
    customer_id,
    age,
    income,
    credit_score,
    predict_churn(age, income, credit_score) AS churn_probability
FROM customers;
```

### 30. Cost Optimization Strategies

#### 30.1 Cost Monitoring and Alerts
```sql
-- Daily credit usage by warehouse
CREATE VIEW daily_credit_usage AS
SELECT 
    DATE(start_time) AS usage_date,
    warehouse_name,
    SUM(credits_used) AS daily_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
GROUP BY usage_date, warehouse_name;

-- Alert on anomalous usage
CREATE VIEW high_credit_alerts AS
SELECT 
    usage_date,
    warehouse_name,
    daily_credits,
    AVG(daily_credits) OVER (
        PARTITION BY warehouse_name 
        ORDER BY usage_date 
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS avg_last_7_days,
    CASE 
        WHEN daily_credits > avg_last_7_days * 1.5 THEN 'HIGH'
        ELSE 'NORMAL'
    END AS alert_level
FROM daily_credit_usage;

-- Storage costs
SELECT 
    usage_date,
    storage_bytes / POWER(1024, 4) AS storage_tb,
    stage_bytes / POWER(1024, 4) AS stage_tb,
    failsafe_bytes / POWER(1024, 4) AS failsafe_tb
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE usage_date >= DATEADD(month, -3, CURRENT_TIMESTAMP())
ORDER BY usage_date DESC;
```

#### 30.2 Cost Optimization Techniques
```sql
-- Identify tables with high storage but low usage
WITH table_usage AS (
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        active_bytes / POWER(1024, 3) AS active_gb,
        time_travel_bytes / POWER(1024, 3) AS time_travel_gb,
        failsafe_bytes / POWER(1024, 3) AS failsafe_gb
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    WHERE deleted IS NULL
),
query_usage AS (
    SELECT 
        database_name,
        schema_name,
        PARSE_JSON(base_objects_accessed)[0]:objectName::STRING AS table_name,
        COUNT(DISTINCT query_id) AS query_count
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    WHERE query_start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3
)
SELECT 
    t.*,
    COALESCE(q.query_count, 0) AS monthly_queries,
    t.active_gb + t.time_travel_gb + t.failsafe_gb AS total_gb
FROM table_usage t
LEFT JOIN query_usage q 
    ON t.table_catalog = q.database_name
   AND t.table_schema = q.schema_name
   AND t.table_name = q.table_name
WHERE total_gb > 100  -- Large tables
  AND COALESCE(q.query_count, 0) < 10  -- Rarely queried
ORDER BY total_gb DESC;

-- Recommendations
-- 1. Drop unused tables
-- 2. Convert to transient tables (no fail-safe)
-- 3. Reduce data retention period
-- 4. Archive to cheaper storage
```

---

## CERTIFICATION PATH

### SnowPro Core Certification

#### Study Areas
1. Snowflake Architecture (15%)
   - Cloud data platform overview
   - Virtual warehouses and compute
   - Storage layer
   - Cloud services layer

2. Account Access and Security (20%)
   - Multi-factor authentication
   - Network policies
   - Key pair authentication
   - OAuth integration
   - Federated authentication

3. Performance Concepts (10%)
   - Query optimization
   - Caching layers
   - Clustering keys
   - Search optimization

4. Data Loading and Unloading (15%)
   - Bulk loading with COPY
   - Continuous loading with Snowpipe
   - Data unloading
   - Stages and file formats

5. Data Transformations (20%)
   - SQL operations
   - Semi-structured data handling
   - Streams and tasks
   - Stored procedures and UDFs

6. Data Protection (15%)
   - Time Travel
   - Fail-safe
   - Cloning
   - Data encryption

7. Data Sharing (5%)
   - Secure data sharing
   - Reader accounts
   - Data marketplace

#### Exam Details
- Duration: 115 minutes
- Questions: 100 multiple choice/multiple select
- Passing score: 750/1000
- Cost: $175 USD
- Format: Remote proctored or testing center

### Advanced Certifications

#### SnowPro Advanced: Data Engineer
- Deep dive into data engineering patterns
- Advanced optimization techniques
- Complex ETL/ELT pipelines
- Performance tuning

#### SnowPro Advanced: Architect
- Multi-account architectures
- Advanced security patterns
- High availability and disaster recovery
- Cost optimization strategies

#### SnowPro Advanced: Data Scientist
- Snowpark for ML
- Feature engineering
- Model deployment
- ML pipelines

---

## HANDS-ON PROJECTS

### Project 1: Build Complete ETL Pipeline
**Objective**: Create production-grade ETL from S3 to Snowflake
- Set up S3 external stage with auto-ingest
- Implement Snowpipe for continuous loading
- Create transformation logic with tasks
- Build aggregation tables
- Implement data quality checks
- Set up monitoring and alerts

### Project 2: Implement SCD Type 2
**Objective**: Track historical changes in dimension tables
- Design dimension table with versioning
- Implement MERGE logic for updates
- Handle inserts, updates, and deletes
- Create views for current and historical data
- Test with sample data changes

### Project 3: Build Data Sharing Solution
**Objective**: Share data securely with external parties
- Create secure views with row-level security
- Implement column masking for PII
- Set up data share
- Add consumer accounts
- Monitor usage

### Project 4: Optimize Large Dataset
**Objective**: Improve performance on billion-row table
- Analyze current query patterns
- Implement appropriate clustering keys
- Enable search optimization
- Create materialized views
- Compare before/after performance

### Project 5: Implement Multi-Tenant Application
**Objective**: Design scalable SaaS data architecture
- Choose isolation strategy (database/schema/row-level)
- Implement row access policies
- Set up tenant-specific warehouses
- Monitor per-tenant costs
- Implement tenant onboarding automation

---

## ADDITIONAL RESOURCES

### Official Documentation
- [Snowflake Documentation](https://docs.snowflake.com)
- [Snowflake Community](https://community.snowflake.com)
- [Snowflake University](https://learn.snowflake.com)

### Best Practices
- Always use TRANSIENT tables for staging/temporary data
- Set appropriate auto-suspend times (5-10 minutes)
- Use result caching to save costs
- Implement proper clustering for large tables
- Use secure views for data sharing
- Monitor credit usage regularly
- Leverage zero-copy cloning for dev/test
- Use streams and tasks for incremental processing
- Tag sensitive data appropriately
- Implement row access policies for data governance

### Common Pitfalls to Avoid
- Over-clustering tables (max 3-4 columns)
- Not using appropriate warehouse sizes
- Forgetting to suspend warehouses
- Not leveraging result caching
- Using SELECT * in production
- Ignoring query profiles
- Not monitoring costs
- Over-privileging users
- Not using version control for SQL
- Ignoring Time Travel for recovery

---

**Estimated Learning Timeline:**
- **Beginner**: 2-3 weeks (basics, SQL, loading data)
- **Intermediate**: 4-6 weeks (streams, tasks, optimization)
- **Advanced**: 8-12 weeks (security, sharing, advanced patterns)
- **Expert**: 6-12 months (production experience, all features)

**Total to Proficiency**: 6-12 months with consistent hands-on practice