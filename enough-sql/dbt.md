# dbt (data build tool) Learning Path: Beginner to Expert

*A comprehensive guide for mastering dbt for analytics engineering*

---
## chapter 1 to 14 first attempt (13 minimum)
## Table of Contents
1. [Beginner Level](#beginner-level)
2. [Intermediate Level](#intermediate-level)
3. [Advanced Level](#advanced-level)
4. [Expert Level](#expert-level)
5. [Certification Path](#certification-path)
6. [Hands-On Projects](#hands-on-projects)

---

## BEGINNER LEVEL

### 1. dbt Fundamentals

#### 1.1 What is dbt?
- **dbt (data build tool)**: transformation framework for data warehouses
- Philosophy: "Select is all you need" - transformations through SQL SELECT statements
- ELT not ETL: Extract-Load-Transform (transformations happen in the warehouse)
- Enables software engineering best practices for analytics
- Open source core + commercial Cloud/Enterprise offerings

#### 1.2 Core Concepts
- **Models**: SQL SELECT statements that create tables or views
- **Sources**: Raw data tables loaded by external tools (Fivetran, Airbyte, etc.)
- **Tests**: Assertions about your data quality
- **Documentation**: Automatically generated and living documentation
- **Snapshots**: Type 2 slowly changing dimension tables
- **Seeds**: CSV files loaded into the warehouse
- **Macros**: Reusable SQL snippets (Jinja templating)
- **Packages**: Reusable dbt code from the community

#### 1.3 dbt Workflow
```
Raw Data → Sources → Staging Models → Intermediate Models → Marts → BI Tools
```

1. **Sources**: Define raw data tables
2. **Staging**: Light transformations, renaming, casting
3. **Intermediate**: Business logic, joins, aggregations
4. **Marts**: Final dimensional/fact tables for BI
5. **BI Layer**: Looker, Tableau, Power BI consume marts

#### 1.4 Why Use dbt?
- Version control for analytics code (Git)
- Testing framework for data quality
- Documentation as code
- DRY (Don't Repeat Yourself) principle with macros
- Dependency management between models
- Lineage tracking
- Modular, maintainable transformations
- CI/CD for analytics

### 2. Installation and Setup

#### 2.1 Installation Options
```bash
# Option 1: dbt Core (open source) via pip
pip install dbt-core
pip install dbt-snowflake  # or dbt-postgres, dbt-bigquery, dbt-redshift

# Option 2: dbt Cloud (managed service)
# Sign up at cloud.getdbt.com

# Option 3: Install with adapter
pip install dbt-snowflake==1.7.0
```

#### 2.2 Supported Data Warehouses
- **Snowflake**: dbt-snowflake
- **BigQuery**: dbt-bigquery
- **Redshift**: dbt-redshift
- **Postgres**: dbt-postgres
- **Databricks**: dbt-databricks
- **Spark**: dbt-spark
- **Others**: 20+ community adapters

#### 2.3 Initialize New dbt Project
```bash
# Create new project
dbt init my_project

# Project structure created:
my_project/
├── dbt_project.yml       # Project configuration
├── models/               # SQL model files
│   └── example/
├── analyses/            # Ad-hoc analyses
├── tests/               # Custom generic tests
├── seeds/               # CSV files
├── macros/              # Jinja macros
├── snapshots/           # Snapshot definitions
└── README.md
```

#### 2.4 Configure profiles.yml
**Location**: `~/.dbt/profiles.yml` (local) or configured in dbt Cloud

```yaml
# Snowflake example
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: xy12345.us-east-1
      user: dbt_user
      password: "{{ env_var('DBT_PASSWORD') }}"
      role: transformer
      database: analytics
      warehouse: transforming
      schema: dbt_dev_schema
      threads: 4

    prod:
      type: snowflake
      account: xy12345.us-east-1
      user: dbt_prod_user
      password: "{{ env_var('DBT_PROD_PASSWORD') }}"
      role: transformer
      database: analytics
      warehouse: transforming
      schema: analytics
      threads: 8

# BigQuery example
my_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: my-gcp-project
      dataset: dbt_dev
      threads: 4
      keyfile: /path/to/keyfile.json
      location: US

# Postgres example
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: analytics
      schema: dbt_dev
      threads: 4
```

#### 2.5 dbt_project.yml Configuration
```yaml
name: 'my_analytics_project'
version: '1.0.0'
config-version: 2

profile: 'my_project'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Global configurations
models:
  my_analytics_project:
    +materialized: view

    staging:
      +materialized: view
      +schema: staging

    intermediate:
      +materialized: ephemeral
      +schema: intermediate

    marts:
      +materialized: table
      +schema: marts

# Testing configuration
tests:
  +store_failures: true

# Documentation
docs-paths: ["docs"]

# Vars (variables)
vars:
  start_date: '2020-01-01'
  exclude_test_data: true
```

### 3. Your First dbt Model

#### 3.1 Basic Model Structure
**File**: `models/staging/stg_customers.sql`

```sql
-- Model: stg_customers
-- Description: Staging layer for raw customer data

{{ config(
    materialized='view',
    schema='staging'
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at

from {{ source('raw', 'customers') }}
```

#### 3.2 Model Configuration
**Three ways to configure models:**

1. **In-file config block** (highest precedence)
```sql
{{ config(
    materialized='table',
    schema='staging',
    tags=['daily', 'pii']
) }}

select ...
```

2. **dbt_project.yml** (project-wide)
```yaml
models:
  my_project:
    staging:
      +materialized: view
```

3. **Model-specific properties** (in schema.yml)
```yaml
models:
  - name: stg_customers
    config:
      materialized: table
```

#### 3.3 Materializations

**View** (default)
```sql
{{ config(materialized='view') }}

select ...
```
- Creates database view
- Fast to build, query hits underlying tables
- Good for staging models

**Table**
```sql
{{ config(materialized='table') }}

select ...
```
- Creates physical table
- Slower to build, fast to query
- Good for marts/final tables

**Incremental**
```sql
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select ...
from source_table

{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```
- Adds/updates only new records
- Good for large fact tables

**Ephemeral**
```sql
{{ config(materialized='ephemeral') }}

select ...
```
- Not materialized, inlined as CTE
- Good for intermediate transformations
- No storage cost

### 4. Running dbt Commands

#### 4.1 Core Commands
```bash
# Test database connection
dbt debug

# Compile models (don't run)
dbt compile

# Run all models
dbt run

# Run specific model
dbt run --select stg_customers

# Run model and all downstream models
dbt run --select stg_customers+

# Run model and all upstream models
dbt run --select +stg_customers

# Run models with tag
dbt run --select tag:daily

# Run models in folder
dbt run --select staging

# Full refresh (rebuild incremental models from scratch)
dbt run --full-refresh

# Run in production
dbt run --target prod
```

#### 4.2 Testing Commands
```bash
# Run all tests
dbt test

# Test specific model
dbt test --select stg_customers

# Test data tests only (not schema tests)
dbt test --data

# Test schema tests only
dbt test --schema
```

#### 4.3 Documentation Commands
```bash
# Generate documentation
dbt docs generate

# Serve documentation locally
dbt docs serve

# Serves on http://localhost:8080
```

#### 4.4 Other Useful Commands
```bash
# List all models
dbt list

# List models with details
dbt list --select staging --output json

# Show SQL for a model
dbt show --select stg_customers

# Clean target directory
dbt clean

# Seed CSV files
dbt seed

# Run snapshots
dbt snapshot

# Compile and show compiled SQL
dbt compile --select stg_customers
```

### 5. Sources

#### 5.1 Defining Sources
**File**: `models/staging/sources.yml`

```yaml
version: 2

sources:
  - name: raw
    description: Raw data loaded by Fivetran
    database: raw_data
    schema: public

    tables:
      - name: customers
        description: Raw customer data from production database
        columns:
          - name: customer_id
            description: Primary key
            tests:
              - unique
              - not_null

      - name: orders
        description: Raw orders data
        loaded_at_field: _fivetran_synced
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
        columns:
          - name: order_id
            tests:
              - unique
              - not_null
          - name: customer_id
            tests:
              - not_null
```

#### 5.2 Using Sources in Models
```sql
-- Using source() function
select
    customer_id,
    first_name,
    last_name
from {{ source('raw', 'customers') }}

-- Generates SQL like:
-- from raw_data.public.customers
```

#### 5.3 Source Freshness
```bash
# Check source freshness
dbt source freshness

# Check specific source
dbt source freshness --select source:raw
```

### 6. Tests

#### 6.1 Schema Tests (Built-in)
**File**: `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_customers
    description: Staged customer data
    columns:
      - name: customer_id
        description: Primary key
        tests:
          - unique
          - not_null

      - name: email
        description: Customer email address
        tests:
          - not_null
          - unique

      - name: customer_status
        description: Customer status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']

      - name: created_at
        description: Account creation timestamp
        tests:
          - not_null

  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null

      - name: customer_id
        description: Foreign key to customers
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
```

#### 6.2 Custom Generic Tests
**File**: `tests/generic/test_positive_value.sql`

```sql
{% test positive_value(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
```

**Usage**:
```yaml
models:
  - name: stg_orders
    columns:
      - name: order_amount
        tests:
          - positive_value
```

#### 6.3 Singular Data Tests
**File**: `tests/assert_total_revenue_matches.sql`

```sql
-- Test that sum of orders equals sum in revenue table
select
    'orders' as source,
    sum(amount) as total
from {{ ref('stg_orders') }}

union all

select
    'revenue' as source,
    sum(revenue) as total
from {{ ref('fct_revenue') }}

having count(distinct total) > 1
```

#### 6.4 Test Configurations
```yaml
models:
  - name: stg_customers
    columns:
      - name: email
        tests:
          - unique:
              severity: error
              warn_if: "> 10"
              error_if: "> 100"
          - not_null:
              where: "customer_status = 'active'"
```

### 7. Documentation

#### 7.1 Model Documentation
**File**: `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_customers
    description: |
      Staged customer data from the production database.

      This model:
      - Cleans and standardizes customer information
      - Applies business rules for customer status
      - Removes test accounts

      **Refresh cadence**: Every 6 hours
      **Owner**: Analytics Team

    columns:
      - name: customer_id
        description: Unique identifier for each customer
        meta:
          dimension:
            type: primary_key

      - name: first_name
        description: Customer's first name (PII)
        meta:
          contains_pii: true

      - name: lifetime_value
        description: |
          Total revenue generated by this customer.
          Calculated as sum of all completed orders.
```

#### 7.2 Doc Blocks
**File**: `models/staging/docs.md`

```markdown
{% docs customer_status %}

Customer status can be one of:
- **active**: Customer has made a purchase in the last 90 days
- **inactive**: Customer has not purchased in 90+ days
- **pending**: New customer who hasn't completed first order

{% enddocs %}

{% docs order_states %}

Order lifecycle states:
1. **pending**: Order created but not paid
2. **processing**: Payment received, preparing shipment
3. **shipped**: Order shipped to customer
4. **delivered**: Customer received order
5. **cancelled**: Order cancelled by customer or system

{% enddocs %}
```

**Reference in schema.yml**:
```yaml
models:
  - name: stg_customers
    columns:
      - name: customer_status
        description: "{{ doc('customer_status') }}"

  - name: stg_orders
    columns:
      - name: order_state
        description: "{{ doc('order_states') }}"
```

#### 7.3 Generate and View Documentation
```bash
# Generate documentation site
dbt docs generate

# Serve locally at http://localhost:8080
dbt docs serve
```

**Documentation includes**:
- Lineage graphs (DAG)
- Column descriptions
- Tests
- Source freshness
- Model code
- Compiled SQL

### 8. Refs and Dependencies

#### 8.1 The ref() Function
```sql
-- models/staging/stg_customers.sql
select * from {{ source('raw', 'customers') }}

-- models/staging/stg_orders.sql
select * from {{ source('raw', 'orders') }}

-- models/marts/fct_orders.sql
select
    o.order_id,
    o.order_date,
    c.customer_name,
    o.amount
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
```

**Why ref() instead of table names?**
- dbt manages dependencies automatically
- Models run in correct order
- Works across environments (dev/prod schemas)
- Enables lineage tracking

#### 8.2 Dependency Graph (DAG)
```
sources
  ├── raw.customers → stg_customers
  │                       └── fct_orders
  │                       └── dim_customers
  └── raw.orders → stg_orders
                      └── fct_orders
```

dbt determines execution order automatically:
1. `stg_customers` and `stg_orders` can run in parallel
2. `fct_orders` runs after both staging models complete

#### 8.3 Model Selection Syntax
```bash
# Run specific model
dbt run --select stg_customers

# Run model and all children (downstream)
dbt run --select stg_customers+

# Run model and all parents (upstream)
dbt run --select +fct_orders

# Run model, parents, and children
dbt run --select +fct_orders+

# Run all staging models
dbt run --select staging

# Run models with specific tag
dbt run --select tag:daily

# Run models by path
dbt run --select path:marts/finance

# Exclude models
dbt run --exclude stg_customers

# Complex selection
dbt run --select +fct_orders --exclude stg_customers
```

---

## INTERMEDIATE LEVEL

### 9. Jinja and Macros

#### 9.1 Jinja Basics in dbt
```sql
-- Variables
{% set payment_methods = ['credit_card', 'debit_card', 'paypal'] %}

-- Loops
select
    order_id,
    {% for method in payment_methods %}
    sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount
    {{ "," if not loop.last }}
    {% endfor %}
from {{ ref('stg_orders') }}
group by order_id

-- Conditionals
select
    order_id,
    {% if target.name == 'prod' %}
    customer_id
    {% else %}
    'REDACTED' as customer_id
    {% endif %}
from {{ ref('stg_orders') }}
```

#### 9.2 Built-in Jinja Variables
```sql
-- target: Information about the target environment
{{ target.name }}        -- 'dev' or 'prod'
{{ target.schema }}      -- Current schema
{{ target.type }}        -- 'snowflake', 'bigquery', etc.
{{ target.database }}    -- Current database
{{ target.threads }}     -- Number of threads

-- this: Current model being built
{{ this }}               -- Fully qualified table name
{{ this.schema }}        -- Schema of current model
{{ this.database }}      -- Database of current model
{{ this.identifier }}    -- Table/view name

-- modules: Useful Jinja functions
{{ modules.datetime.datetime.now() }}
{{ modules.re.sub('[^a-zA-Z]', '', 'abc123') }}

-- var(): Access variables from dbt_project.yml
{{ var('start_date') }}
{{ var('exclude_test', true) }}  -- With default value

-- env_var(): Access environment variables
{{ env_var('DBT_PASSWORD') }}
{{ env_var('API_KEY', 'default_value') }}
```

#### 9.3 Creating Macros
**File**: `macros/cents_to_dollars.sql`

```sql
{% macro cents_to_dollars(column_name, precision=2) %}
    round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
```

**Usage**:
```sql
select
    order_id,
    {{ cents_to_dollars('amount_cents') }} as amount_dollars,
    {{ cents_to_dollars('tax_cents', 3) }} as tax_dollars
from {{ ref('stg_orders') }}
```

#### 9.4 Advanced Macro Examples
**Generate date spine**:
```sql
-- macros/generate_date_spine.sql
{% macro generate_date_spine(start_date, end_date) %}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="cast('" ~ end_date ~ "' as date)"
    ) }}
)

select
    cast(date_day as date) as date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day
from date_spine

{% endmacro %}
```

**Pivot table macro**:
```sql
-- macros/pivot.sql
{% macro pivot(column, values, agg_func='sum', cmp_column='', cmp_value='', else_value=0) %}

{% for value in values %}
    {{ agg_func }}(
        case
            when {{ column }} = '{{ value }}'
            {% if cmp_column %} and {{ cmp_column }} = '{{ cmp_value }}' {% endif %}
            then 1
            else {{ else_value }}
        end
    ) as {{ value | replace(' ', '_') | lower }}
    {{ "," if not loop.last }}
{% endfor %}

{% endmacro %}
```

**Usage**:
```sql
select
    date,
    {{ pivot('payment_method', ['credit_card', 'debit_card', 'paypal'], 'sum', 'amount') }}
from {{ ref('stg_payments') }}
group by date
```

#### 9.5 dbt Utils Package Macros
```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

```bash
dbt deps  # Install packages
```

**Common dbt_utils macros**:
```sql
-- Generate surrogate key
{{ dbt_utils.generate_surrogate_key(['customer_id', 'order_id']) }}

-- Union tables
{{ dbt_utils.union_relations(relations=[ref('table1'), ref('table2')]) }}

-- Get column values as list
{% set payment_methods = dbt_utils.get_column_values(
    table=ref('stg_orders'),
    column='payment_method'
) %}

-- Pivot
{{ dbt_utils.pivot(
    column='payment_method',
    values=payment_methods,
    agg='sum',
    cmp='amount',
    prefix='payment_',
    suffix='_total'
) }}

-- Date spine
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="'2020-01-01'",
    end_date="current_date"
) }}

-- Group by (generate GROUP BY 1,2,3...)
{{ dbt_utils.group_by(n=3) }}

-- Star (select all columns except specified)
{{ dbt_utils.star(from=ref('stg_customers'), except=['password', 'ssn']) }}
```

### 10. Incremental Models

#### 10.1 Basic Incremental Model
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    order_id,
    customer_id,
    order_date,
    amount,
    updated_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
    -- Only new or updated records
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**How it works**:
- First run: Full table build (like table materialization)
- Subsequent runs: Only process new/changed records
- dbt merges/appends based on `unique_key`

#### 10.2 Incremental Strategies

**Merge** (default for most warehouses)
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select ...
```
- Updates existing records, inserts new ones
- Supports updates and deletes
- Slower but more flexible

**Append** (fastest)
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='append'
) }}

select ...
```
- Only inserts new records
- No updates or deletes
- Fastest for append-only data (logs, events)

**Insert Overwrite** (partition-aware)
```sql
{{ config(
    materialized='incremental',
    unique_key='date_day',
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'date_day',
        'data_type': 'date'
    }
) }}

select ...
```
- Overwrites entire partitions
- Good for BigQuery/Spark
- Handles late-arriving data

**Delete+Insert**
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

select ...
```
- Deletes matching records, then inserts
- More predictable than merge
- Good for Postgres/Redshift

#### 10.3 Advanced Incremental Patterns
**Late-arriving data handling**:
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select ...
from {{ ref('stg_orders') }}

{% if is_incremental() %}
    -- Look back 3 days to catch late arrivals
    where order_date > (select max(order_date) - interval '3 days' from {{ this }})
{% endif %}
```

**Incremental with hard deletes**:
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    merge_update_columns=['status', 'updated_at']
) }}

select
    order_id,
    customer_id,
    status,
    updated_at,
    case when status = 'deleted' then true else false end as is_deleted
from {{ ref('stg_orders') }}

{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

#### 10.4 Full Refresh
```bash
# Rebuild incremental model from scratch
dbt run --full-refresh --select fct_orders

# Full refresh all incremental models
dbt run --full-refresh
```

### 11. Snapshots (SCD Type 2)

#### 11.1 Basic Snapshot
**File**: `snapshots/customers_snapshot.sql`

```sql
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
```

**Generated columns**:
- `dbt_valid_from`: When this version became active
- `dbt_valid_to`: When this version became inactive (NULL if current)
- `dbt_updated_at`: Timestamp of last update
- `dbt_scd_id`: Unique identifier for each version

#### 11.2 Snapshot Strategies

**Timestamp Strategy**
```sql
{% snapshot orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select * from {{ ref('stg_orders') }}

{% endsnapshot %}
```
- Compares `updated_at` column
- Creates new version when timestamp changes

**Check Strategy**
```sql
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email', 'phone', 'address']
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
```
- Compares specified columns
- Creates new version when any column changes

**Check All Columns**
```sql
{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols='all'
    )
}}

select * from {{ ref('stg_products') }}

{% endsnapshot %}
```

#### 11.3 Running Snapshots
```bash
# Run all snapshots
dbt snapshot

# Run specific snapshot
dbt snapshot --select customers_snapshot

# Check snapshot history
select * from snapshots.customers_snapshot
where customer_id = 123
order by dbt_valid_from
```

#### 11.4 Querying Snapshots
```sql
-- Get current state (Type 1)
select *
from {{ ref('customers_snapshot') }}
where dbt_valid_to is null

-- Get historical state at specific date
select *
from {{ ref('customers_snapshot') }}
where '2024-01-01' between dbt_valid_from and coalesce(dbt_valid_to, '9999-12-31')

-- Count versions per customer
select
    customer_id,
    count(*) as version_count,
    min(dbt_valid_from) as first_seen,
    max(coalesce(dbt_valid_to, current_timestamp)) as last_seen
from {{ ref('customers_snapshot') }}
group by customer_id
having count(*) > 1  -- Only customers with changes
```

### 12. Seeds

#### 12.1 What are Seeds?
- CSV files in your dbt project
- Loaded into warehouse as tables
- Good for: lookup tables, mappings, small reference data
- NOT for large datasets (use sources instead)

#### 12.2 Creating Seeds
**File**: `seeds/country_codes.csv`
```csv
country_code,country_name,region
US,United States,North America
CA,Canada,North America
UK,United Kingdom,Europe
DE,Germany,Europe
JP,Japan,Asia
```

**File**: `seeds/payment_method_mapping.csv`
```csv
raw_payment_method,clean_payment_method,category
cc,Credit Card,card
debit,Debit Card,card
paypal,PayPal,digital_wallet
venmo,Venmo,digital_wallet
```

#### 12.3 Seed Configuration
**In dbt_project.yml**:
```yaml
seeds:
  my_project:
    +schema: reference
    +quote_columns: false

    country_codes:
      +column_types:
        country_code: varchar(2)
        country_name: varchar(100)
```

**In seed properties**:
```yaml
# seeds/properties.yml
version: 2

seeds:
  - name: country_codes
    description: ISO country codes and regions
    config:
      column_types:
        country_code: varchar(2)
    columns:
      - name: country_code
        description: ISO 3166-1 alpha-2 code
        tests:
          - unique
          - not_null
```

#### 12.4 Using Seeds
```bash
# Load all seeds
dbt seed

# Load specific seed
dbt seed --select country_codes

# Full refresh (truncate and reload)
dbt seed --full-refresh
```

**Reference in models**:
```sql
select
    o.order_id,
    o.payment_method_raw,
    pm.clean_payment_method,
    pm.category
from {{ ref('stg_orders') }} o
left join {{ ref('payment_method_mapping') }} pm
    on o.payment_method_raw = pm.raw_payment_method
```

### 13. Exposures

#### 13.1 Defining Exposures
**File**: `models/marts/exposures.yml`

```yaml
version: 2

exposures:
  - name: weekly_revenue_dashboard
    description: Executive dashboard showing weekly revenue trends
    type: dashboard
    maturity: high
    url: https://looker.company.com/dashboards/123

    owner:
      name: Analytics Team
      email: analytics@company.com

    depends_on:
      - ref('fct_revenue')
      - ref('dim_customers')
      - ref('dim_products')

    tags: ['executive', 'weekly']

  - name: customer_segmentation_report
    description: Monthly customer segmentation analysis
    type: analysis
    maturity: medium

    owner:
      name: Marketing Team
      email: marketing@company.com

    depends_on:
      - ref('fct_customer_metrics')
      - ref('dim_customer_segments')

    tags: ['marketing', 'monthly']

  - name: operational_metrics_app
    description: Real-time operational metrics application
    type: application
    maturity: high
    url: https://app.company.com/ops

    owner:
      name: Operations Team
      email: ops@company.com

    depends_on:
      - ref('fct_orders')
      - ref('fct_shipments')
```

**Exposure types**:
- `dashboard`: BI dashboards (Looker, Tableau, etc.)
- `notebook`: Jupyter, Hex, Deepnote notebooks
- `analysis`: Ad-hoc analyses
- `ml`: Machine learning models
- `application`: Data-powered applications

#### 13.2 Benefits of Exposures
- Track downstream dependencies
- Visualize impact in DAG
- Coordinate with stakeholders
- Document data consumers
- Impact analysis before changes

### 14. Metrics (dbt Semantic Layer)

#### 14.1 Defining Metrics
**File**: `models/marts/metrics.yml`

```yaml
version: 2

metrics:
  - name: total_revenue
    label: Total Revenue
    model: ref('fct_orders')
    description: Sum of all order amounts

    calculation_method: sum
    expression: amount

    timestamp: order_date
    time_grains: [day, week, month, quarter, year]

    dimensions:
      - customer_country
      - payment_method

    filters:
      - field: order_status
        operator: '='
        value: "'completed'"

    meta:
      team: finance

  - name: average_order_value
    label: Average Order Value
    model: ref('fct_orders')
    description: Average value of orders

    calculation_method: average
    expression: amount

    timestamp: order_date
    time_grains: [day, week, month]

    filters:
      - field: order_status
        operator: '='
        value: "'completed'"

  - name: customer_count
    label: Total Customers
    model: ref('dim_customers')
    description: Count of active customers

    calculation_method: count_distinct
    expression: customer_id

    timestamp: created_at
    time_grains: [month, quarter, year]

    filters:
      - field: status
        operator: '='
        value: "'active'"
```

#### 14.2 Calculation Methods
- `count`: Count of rows
- `count_distinct`: Distinct count
- `sum`: Sum of expression
- `average`: Average of expression
- `min`: Minimum value
- `max`: Maximum value
- `median`: Median value (if supported by warehouse)

#### 14.3 Derived Metrics
```yaml
metrics:
  - name: revenue_per_customer
    label: Revenue per Customer
    description: Average revenue per active customer

    calculation_method: derived
    expression: "{{ metric('total_revenue') }} / {{ metric('customer_count') }}"

    timestamp: order_date
    time_grains: [month, quarter, year]
```

---

## ADVANCED LEVEL

### 15. Advanced Project Structure

#### 15.1 Recommended Folder Organization
```
models/
├── staging/                 # Source system staging
│   ├── crm/                # CRM system (Salesforce)
│   │   ├── _crm__sources.yml
│   │   ├── _crm__models.yml
│   │   ├── stg_crm__accounts.sql
│   │   ├── stg_crm__contacts.sql
│   │   └── stg_crm__opportunities.sql
│   ├── ecommerce/          # E-commerce platform
│   │   ├── _ecommerce__sources.yml
│   │   ├── _ecommerce__models.yml
│   │   ├── stg_ecommerce__customers.sql
│   │   ├── stg_ecommerce__orders.sql
│   │   └── stg_ecommerce__products.sql
│   └── payments/           # Payment processor
│       ├── _payments__sources.yml
│       ├── _payments__models.yml
│       ├── stg_payments__transactions.sql
│       └── stg_payments__refunds.sql
│
├── intermediate/           # Business logic layer
│   ├── finance/
│   │   ├── int_revenue__pivoted.sql
│   │   ├── int_revenue__reconciled.sql
│   │   └── _int_finance__models.yml
│   ├── marketing/
│   │   ├── int_customer__enriched.sql
│   │   ├── int_campaign__performance.sql
│   │   └── _int_marketing__models.yml
│   └── product/
│       ├── int_user__engagement.sql
│       └── _int_product__models.yml
│
└── marts/                  # Business-facing models
    ├── core/              # Company-wide KPIs
    │   ├── _core__models.yml
    │   ├── dim_customers.sql
    │   ├── dim_products.sql
    │   ├── fct_orders.sql
    │   └── fct_revenue.sql
    ├── finance/           # Finance-specific
    │   ├── _finance__models.yml
    │   ├── fct_daily_revenue.sql
    │   ├── fct_monthly_mrr.sql
    │   └── rpt_revenue_by_product.sql
    ├── marketing/         # Marketing-specific
    │   ├── _marketing__models.yml
    │   ├── fct_customer_acquisitions.sql
    │   ├── fct_campaigns.sql
    │   └── rpt_campaign_roi.sql
    └── product/          # Product-specific
        ├── _product__models.yml
        ├── fct_user_sessions.sql
        ├── fct_feature_usage.sql
        └── rpt_product_metrics.sql
```

#### 15.2 Naming Conventions
**Staging models**: `stg_<source>__<entity>.sql`
- `stg_salesforce__accounts.sql`
- `stg_stripe__invoices.sql`

**Intermediate models**: `int_<entity>__<verb>.sql`
- `int_orders__pivoted.sql`
- `int_customers__enriched.sql`

**Fact tables**: `fct_<entity>.sql` or `fct_<process>.sql`
- `fct_orders.sql`
- `fct_revenue.sql`

**Dimension tables**: `dim_<entity>.sql`
- `dim_customers.sql`
- `dim_products.sql`

**Reports**: `rpt_<description>.sql`
- `rpt_monthly_revenue.sql`
- `rpt_customer_cohorts.sql`

#### 15.3 Model Layering Best Practices
**Staging Layer**:
- One model per source table
- Light transformations only:
  - Renaming columns to standard names
  - Type casting
  - Basic deduplication
- Always `materialized: view`
- No joins between sources

**Intermediate Layer**:
- Business logic and complex transformations
- Joins between staging models
- Usually `materialized: ephemeral` or `view`
- Not exposed to end users

**Marts Layer**:
- Business-conformed, denormalized models
- Ready for BI tools
- Usually `materialized: table` or `incremental`
- Well-documented and tested

### 16. Testing Strategies

#### 16.1 Test Coverage Strategy
```yaml
# Staging models - basic tests
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
      - name: order_date
        tests:
          - not_null

# Marts - comprehensive tests
  - name: fct_orders
    description: Fact table of all orders
    tests:
      # Relationship tests
      - dbt_utils.expression_is_true:
          expression: "revenue >= 0"

      # Recency test
      - dbt_utils.recency:
          datepart: day
          field: order_date
          interval: 1

    columns:
      - name: order_id
        tests:
          - unique
          - not_null

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id

      - name: revenue
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
```

#### 16.2 Great Expectations for dbt
```yaml
# packages.yml
packages:
  - package: calogica/dbt_expectations
    version: 0.10.0
```

```bash
dbt deps
```

**Advanced tests**:
```yaml
models:
  - name: fct_orders
    columns:
      - name: revenue
        tests:
          # Statistical tests
          - dbt_expectations.expect_column_mean_to_be_between:
              min_value: 50
              max_value: 200

          # Null percentage
          - dbt_expectations.expect_column_null_rate_to_be_between:
              max_value: 0.05

          # Value distribution
          - dbt_expectations.expect_column_quantile_values_to_be_between:
              quantile: 0.95
              min_value: 0
              max_value: 5000

      - name: order_date
        tests:
          # Date validity
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "'2020-01-01'"
              max_value: "current_date + interval '1 day'"

      - name: email
        tests:
          # Regex pattern
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

#### 16.3 Custom Test Configurations
```yaml
# dbt_project.yml
tests:
  my_project:
    +store_failures: true
    +severity: warn

    staging:
      +severity: error

    marts:
      +store_failures: true
      +severity: error
```

**Test results storage**:
```sql
-- Query failed test results
select * from {{ target.schema }}_dbt_test__audit.unique_stg_orders_order_id
where failed_at is not null
```

#### 16.4 dbt-checkpoint (Pre-commit Hooks)
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/dbt-checkpoint/dbt-checkpoint
    rev: v1.2.0
    hooks:
      - id: check-model-has-all-columns
      - id: check-model-has-description
      - id: check-model-has-tests
        args: ["--test-cnt", "2", "--"]
      - id: check-column-desc-are-same
      - id: check-column-name-contract
      - id: dbt-compile
      - id: dbt-test
```

### 17. Advanced Macros and Packages

#### 17.1 Creating Reusable Macro Library
**File**: `macros/sql_helpers.sql`

```sql
-- Generate date dimension table
{% macro generate_date_dimension(start_date, end_date) %}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'" ~ start_date ~ "'::date",
        end_date="'" ~ end_date ~ "'::date"
    ) }}
),

final as (
    select
        date_day,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(week from date_day) as week,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        date_day = last_day(date_day) as is_last_day_of_month,
        case extract(dayofweek from date_day)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,
        case
            when extract(dayofweek from date_day) in (0, 6) then true
            else false
        end as is_weekend
    from date_spine
)

select * from final

{% endmacro %}
```

**File**: `macros/get_custom_schema.sql`
```sql
-- Override default schema naming
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

#### 17.2 Essential dbt Packages
```yaml
# packages.yml
packages:
  # Core utilities
  - package: dbt-labs/dbt_utils
    version: 1.1.1

  # Data quality tests
  - package: calogica/dbt_expectations
    version: 0.10.0

  # Date/time utilities
  - package: calogica/dbt_date
    version: 0.10.0

  # Audit helper (compare datasets)
  - package: dbt-labs/audit_helper
    version: 0.9.0

  # Codegen (generate boilerplate)
  - package: dbt-labs/codegen
    version: 0.12.0

  # Snowflake utilities
  - package: dbt-labs/snowflake_utils
    version: 0.1.0
```

#### 17.3 Codegen for Boilerplate
```sql
-- Generate source YAML
{{ codegen.generate_source('raw_data') }}

-- Generate base model YAML
{{ codegen.generate_base_model(
    source_name='raw_data',
    table_name='customers'
) }}

-- Generate model YAML
{{ codegen.generate_model_yaml(
    model_names=['stg_customers', 'stg_orders']
) }}
```

#### 17.4 Audit Helper for Comparisons
```sql
-- Compare two relations
{{ audit_helper.compare_relations(
    a_relation=ref('customers_v1'),
    b_relation=ref('customers_v2'),
    primary_key='customer_id'
) }}

-- Compare column values
{{ audit_helper.compare_column_values(
    a_query="select revenue from " ~ ref('revenue_v1'),
    b_query="select revenue from " ~ ref('revenue_v2'),
    primary_key='date',
    column_to_compare='revenue'
) }}

-- Quick compare all columns
{{ audit_helper.compare_all_columns(
    a_relation=ref('old_model'),
    b_relation=ref('new_model'),
    primary_key='id'
) }}
```

### 18. Performance Optimization

#### 18.1 Model Materialization Strategy
```yaml
# dbt_project.yml
models:
  my_project:
    staging:
      +materialized: view
      +tags: ['staging']

    intermediate:
      +materialized: ephemeral
      +tags: ['intermediate']

    marts:
      core:
        +materialized: table
        +tags: ['marts', 'core']

      finance:
        +materialized: table
        +tags: ['marts', 'finance']

        fct_daily_revenue:
          +materialized: incremental
          +unique_key: date_day
          +on_schema_change: append_new_columns
```

#### 18.2 Incremental Model Best Practices
```sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='fail',
        incremental_strategy='merge',
        cluster_by=['order_date'],
        tags=['incremental', 'daily']
    )
}}

with source as (
    select *
    from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- Look back 3 days for late arrivals
        where order_date > (
            select dateadd(day, -3, max(order_date))
            from {{ this }}
        )
    {% endif %}
),

-- Apply business logic
final as (
    select
        order_id,
        customer_id,
        order_date,
        sum(amount) as revenue,
        count(*) as item_count,
        current_timestamp() as updated_at
    from source
    group by 1, 2, 3
)

select * from final
```

#### 18.3 Query Optimization Techniques
**Use CTEs for readability**:
```sql
with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        o.order_id,
        o.order_date,
        c.customer_name,
        o.amount
    from orders o
    left join customers c using (customer_id)
)

select * from joined
```

**Avoid SELECT \***:
```sql
-- Bad
select * from {{ ref('stg_orders') }}

-- Good
select
    order_id,
    customer_id,
    order_date,
    amount
from {{ ref('stg_orders') }}
```

**Leverage warehouse-specific optimizations**:
```sql
-- Snowflake: Use CLUSTER BY
{{ config(
    cluster_by=['order_date', 'customer_id']
) }}

-- BigQuery: Use PARTITION BY
{{ config(
    partition_by={
        'field': 'order_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['customer_id', 'product_id']
) }}

-- Redshift: Use DIST and SORT keys
{{ config(
    dist='customer_id',
    sort=['order_date', 'customer_id']
) }}
```

#### 18.4 Monitoring Model Performance
```sql
-- Query run_results.json for timing
select
    node_id,
    execution_time,
    status,
    rows_affected
from {{ source('dbt_artifacts', 'model_executions') }}
where execution_time > 300  -- Models taking > 5 minutes
order by execution_time desc
```

### 19. CI/CD and Deployment

#### 19.1 dbt Cloud Jobs
**Development Job**:
- Runs on PR creation
- Builds only modified models + children
- Runs tests on modified models
- Posts results to PR

**Production Job**:
- Scheduled (e.g., daily at 2 AM)
- Full refresh on weekends
- Sends alerts on failure
- Generates documentation

#### 19.2 Slim CI with State Comparison
```bash
# In CI environment
# Compare against production state
dbt run --select state:modified+ --state ./prod_artifacts

# Run tests on modified models only
dbt test --select state:modified+ --state ./prod_artifacts
```

**GitHub Actions example**:
```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt-run:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install dbt-snowflake

      - name: Download production artifacts
        run: |
          # Download manifest.json from prod
          aws s3 cp s3://dbt-artifacts/prod/manifest.json ./prod_artifacts/

      - name: Run dbt
        env:
          DBT_SNOWFLAKE_ACCOUNT: ${{ secrets.DBT_SNOWFLAKE_ACCOUNT }}
          DBT_USER: ${{ secrets.DBT_USER }}
          DBT_PASSWORD: ${{ secrets.DBT_PASSWORD }}
        run: |
          dbt deps
          dbt run --select state:modified+ --state ./prod_artifacts
          dbt test --select state:modified+ --state ./prod_artifacts
```

#### 19.3 Blue/Green Deployments
```sql
-- Create new schema with timestamp
{% set deploy_schema = target.schema ~ '_' ~ modules.datetime.datetime.now().strftime('%Y%m%d_%H%M%S') %}

{{ config(schema=deploy_schema) }}

-- Build models in new schema
-- After validation, swap schemas
-- alter schema analytics rename to analytics_old;
-- alter schema analytics_20240101_120000 rename to analytics;
```

#### 19.4 Environment Management
```yaml
# profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      database: analytics_dev
      schema: "dbt_{{ env_var('USER') }}"
      warehouse: transforming

    ci:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      database: analytics_ci
      schema: dbt_ci_{{ env_var('GITHUB_RUN_ID') }}
      warehouse: transforming

    prod:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_PROD_USER') }}"
      password: "{{ env_var('DBT_PROD_PASSWORD') }}"
      database: analytics
      schema: analytics
      warehouse: transforming
```

---

## EXPERT LEVEL

### 20. Advanced dbt Architecture Patterns

#### 20.1 Feature Store Pattern
```sql
-- models/ml/features/customer_features.sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        tags=['ml', 'features']
    )
}}

with customer_base as (
    select * from {{ ref('dim_customers') }}
),

order_features as (
    select
        customer_id,
        count(*) as total_orders,
        sum(amount) as lifetime_value,
        avg(amount) as avg_order_value,
        max(order_date) as last_order_date,
        datediff(day, max(order_date), current_date()) as days_since_last_order
    from {{ ref('fct_orders') }}
    group by customer_id
),

engagement_features as (
    select
        customer_id,
        count(*) as total_sessions,
        sum(page_views) as total_page_views,
        avg(session_duration) as avg_session_duration
    from {{ ref('fct_sessions') }}
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.signup_date,
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.lifetime_value, 0) as lifetime_value,
        coalesce(o.avg_order_value, 0) as avg_order_value,
        o.days_since_last_order,
        coalesce(e.total_sessions, 0) as total_sessions,
        coalesce(e.avg_session_duration, 0) as avg_session_duration,

        -- Computed features
        case
            when o.total_orders >= 10 then 'high_value'
            when o.total_orders >= 5 then 'medium_value'
            else 'low_value'
        end as customer_segment,

        case
            when o.days_since_last_order <= 30 then 'active'
            when o.days_since_last_order <= 90 then 'at_risk'
            else 'churned'
        end as lifecycle_stage,

        current_timestamp() as feature_timestamp
    from customer_base c
    left join order_features o using (customer_id)
    left join engagement_features e using (customer_id)
)

select * from final
```

#### 20.2 Multi-Tenant SaaS Pattern
```sql
-- Macro for tenant isolation
{% macro get_tenant_filter() %}
    {% if target.name != 'prod' %}
        -- In dev, only show test tenant
        where tenant_id = '{{ var("dev_tenant_id", "test") }}'
    {% else %}
        -- In prod, show all tenants
        where tenant_id is not null
    {% endif %}
{% endmacro %}

-- Model with tenant isolation
select
    tenant_id,
    customer_id,
    order_id,
    amount
from {{ ref('stg_orders') }}
{{ get_tenant_filter() }}
```

#### 20.3 Event Streaming Pattern
```sql
-- models/staging/events/stg_events__enriched.sql
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='append_new_columns'
    )
}}

with raw_events as (
    select * from {{ source('kafka', 'events') }}

    {% if is_incremental() %}
        where event_timestamp > (select max(event_timestamp) from {{ this }})
    {% endif %}
),

parsed as (
    select
        event_id,
        event_timestamp,
        event_type,
        user_id,
        session_id,
        parse_json(event_properties) as properties,

        -- Extract common properties
        properties:page_url::string as page_url,
        properties:referrer::string as referrer,
        properties:utm_source::string as utm_source,
        properties:utm_medium::string as utm_medium,
        properties:device_type::string as device_type
    from raw_events
),

enriched as (
    select
        e.*,
        u.user_segment,
        u.user_country,
        s.session_start_time,
        datediff(second, s.session_start_time, e.event_timestamp) as seconds_into_session
    from parsed e
    left join {{ ref('dim_users') }} u using (user_id)
    left join {{ ref('fct_sessions') }} s using (session_id)
)

select * from enriched
```

### 21. dbt Artifacts and Metadata

#### 21.1 Understanding dbt Artifacts
**Generated files** (in `target/` directory):
- `manifest.json`: Full project graph, all nodes and dependencies
- `run_results.json`: Results of last dbt invocation
- `catalog.json`: Column-level metadata from database
- `sources.json`: Source freshness check results

#### 21.2 Querying dbt Metadata
```sql
-- Load manifest.json into warehouse
-- Can use dbt_artifacts package or custom loader

-- Find most expensive models
with model_timing as (
    select
        node_id,
        execution_time,
        rows_affected,
        execution_time / nullif(rows_affected, 0) as seconds_per_row
    from dbt_artifacts.model_executions
    where status = 'success'
)

select
    node_id,
    avg(execution_time) as avg_execution_time,
    max(execution_time) as max_execution_time,
    avg(rows_affected) as avg_rows,
    avg(seconds_per_row) as avg_seconds_per_row
from model_timing
group by node_id
order by avg_execution_time desc
limit 20;

-- Find models with frequent failures
select
    node_id,
    count(*) as total_runs,
    sum(case when status = 'error' then 1 else 0 end) as error_count,
    sum(case when status = 'error' then 1 else 0 end)::float / count(*) as error_rate
from dbt_artifacts.model_executions
group by node_id
having error_count > 0
order by error_rate desc;
```

#### 21.3 Building Model Performance Dashboard
```sql
-- Daily model execution metrics
create or replace view model_performance_daily as
with daily_stats as (
    select
        date(created_at) as execution_date,
        node_id,
        count(*) as runs,
        avg(execution_time) as avg_execution_time,
        max(execution_time) as max_execution_time,
        sum(case when status = 'error' then 1 else 0 end) as failures
    from dbt_artifacts.model_executions
    group by 1, 2
)

select
    execution_date,
    node_id,
    runs,
    round(avg_execution_time, 2) as avg_seconds,
    round(max_execution_time, 2) as max_seconds,
    failures,
    round(failures::float / runs * 100, 2) as failure_rate_pct
from daily_stats
order by execution_date desc, avg_seconds desc;
```

### 22. Custom Materializations

#### 22.1 Creating Custom Materialization
**File**: `macros/materializations/my_custom_materialization.sql`

```sql
{% materialization custom_incremental, default %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(this) -%}

  -- Build temp table
  {{ run_hooks(pre_hooks) }}

  {% call statement('main') %}
    {{ create_table_as(False, tmp_relation, sql) }}
  {% endcall %}

  -- Handle merge/insert logic
  {% if existing_relation is none %}
    -- First run: create table
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    -- Incremental run: merge
    {% set build_sql %}
      merge into {{ target_relation }} as target
      using {{ tmp_relation }} as source
      on target.id = source.id
      when matched then update set *
      when not matched then insert *
    {% endset %}
  {% endif %}

  {% call statement('main') %}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
```

#### 22.2 Using Custom Materialization
```sql
{{
    config(
        materialized='custom_incremental',
        unique_key='id'
    )
}}

select * from {{ ref('source_table') }}
```

### 23. Advanced Testing Patterns

#### 23.1 Cross-Database Reconciliation
```sql
-- tests/reconcile_revenue.sql
-- Compare dbt-generated revenue with source system

with dbt_revenue as (
    select
        date_trunc('day', order_date) as date,
        sum(amount) as revenue
    from {{ ref('fct_orders') }}
    group by 1
),

source_revenue as (
    select
        date_trunc('day', order_date) as date,
        sum(amount) as revenue
    from {{ source('erp', 'orders') }}
    group by 1
),

comparison as (
    select
        coalesce(d.date, s.date) as date,
        d.revenue as dbt_revenue,
        s.revenue as source_revenue,
        abs(d.revenue - s.revenue) as difference,
        abs(d.revenue - s.revenue) / nullif(s.revenue, 0) * 100 as pct_difference
    from dbt_revenue d
    full outer join source_revenue s using (date)
)

select *
from comparison
where pct_difference > 0.01  -- Flag >0.01% difference
```

#### 23.2 Data Drift Detection
```sql
-- tests/detect_customer_drift.sql
-- Detect unexpected changes in customer distribution

with current_snapshot as (
    select
        country,
        customer_segment,
        count(*) as customer_count,
        current_date() as snapshot_date
    from {{ ref('dim_customers') }}
    group by 1, 2
),

previous_snapshot as (
    select
        country,
        customer_segment,
        customer_count,
        snapshot_date
    from {{ ref('customer_distribution_history') }}
    where snapshot_date = current_date() - 1
),

comparison as (
    select
        coalesce(c.country, p.country) as country,
        coalesce(c.customer_segment, p.customer_segment) as customer_segment,
        c.customer_count as current_count,
        p.customer_count as previous_count,
        (c.customer_count - p.customer_count)::float / nullif(p.customer_count, 0) * 100 as pct_change
    from current_snapshot c
    full outer join previous_snapshot p
        on c.country = p.country
        and c.customer_segment = p.customer_segment
)

select *
from comparison
where abs(pct_change) > 20  -- Flag >20% change
```

### 24. dbt Python Models

#### 24.1 Python Model Basics (dbt 1.3+)
**File**: `models/ml/customer_clustering.py`

```python
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

def model(dbt, session):
    # Get upstream model as DataFrame
    customers_df = dbt.ref("fct_customer_features").to_pandas()

    # Feature engineering
    features = ['total_orders', 'lifetime_value', 'avg_order_value',
                'days_since_last_order', 'total_sessions']

    X = customers_df[features].fillna(0)

    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Cluster customers
    kmeans = KMeans(n_clusters=5, random_state=42)
    customers_df['cluster'] = kmeans.fit_predict(X_scaled)

    # Add cluster statistics
    cluster_stats = customers_df.groupby('cluster')[features].mean()
    cluster_stats.columns = [f'cluster_avg_{col}' for col in features]

    result = customers_df.merge(cluster_stats, on='cluster', how='left')

    return result
```

#### 24.2 Python Model Configuration
```python
def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['pandas', 'scikit-learn'],
        tags=['ml', 'python']
    )

    # Model logic...
    return result
```

#### 24.3 Use Cases for Python Models
- Machine learning model training
- Advanced statistical analysis
- Complex data transformations
- Feature engineering
- Integrations with Python libraries (scikit-learn, statsmodels, etc.)

### 25. Cost Optimization

#### 25.1 Analyzing dbt Costs
```sql
-- Cost per model (Snowflake example)
with model_costs as (
    select
        m.node_id,
        m.execution_time / 3600.0 as hours,
        w.warehouse_size,
        case w.warehouse_size
            when 'X-SMALL' then 1
            when 'SMALL' then 2
            when 'MEDIUM' then 4
            when 'LARGE' then 8
            when 'X-LARGE' then 16
        end as credits_per_hour,
        (execution_time / 3600.0) * credits_per_hour as credits_used,
        credits_used * 3.0 as estimated_cost_usd  -- $3 per credit
    from dbt_artifacts.model_executions m
    join warehouse_info w on m.warehouse_name = w.warehouse_name
)

select
    node_id,
    sum(credits_used) as total_credits,
    sum(estimated_cost_usd) as total_cost_usd,
    count(*) as runs
from model_costs
group by node_id
order by total_cost_usd desc;
```

#### 25.2 Cost Optimization Strategies
**Use incremental models for large tables**:
```sql
-- Before: Full table rebuild daily (expensive)
{{ config(materialized='table') }}

-- After: Incremental (much cheaper)
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}
```

**Use ephemeral for intermediate models**:
```yaml
# dbt_project.yml
models:
  intermediate:
    +materialized: ephemeral  # No warehouse storage
```

**Schedule jobs efficiently**:
```yaml
# dbt Cloud
# Run expensive models only once daily
# Run cheap models hourly
```

**Use smaller warehouses for development**:
```yaml
# profiles.yml
dev:
  warehouse: COMPUTE_WH_XS  # X-Small for dev
prod:
  warehouse: COMPUTE_WH_L   # Large for prod
```

---

## CERTIFICATION PATH

### dbt Analytics Engineering Certification

#### Exam Details
- **Provider**: dbt Labs
- **Cost**: Free
- **Duration**: 90 minutes
- **Format**: Multiple choice
- **Passing Score**: 60%
- **Certificate**: Digital badge

#### Study Areas
1. **dbt Fundamentals** (30%)
   - Models, sources, tests, documentation
   - Project structure and configuration
   - Materializations
   - Jinja and macros

2. **Testing and Documentation** (20%)
   - Schema tests
   - Data tests
   - Documentation best practices
   - dbt docs

3. **Deployment and Operations** (20%)
   - Development workflow
   - Git integration
   - dbt Cloud jobs
   - Environment management

4. **Advanced Concepts** (30%)
   - Incremental models
   - Snapshots
   - Packages and macros
   - Performance optimization
   - Data governance

#### Preparation Resources
- [dbt Learn (free courses)](https://learn.getdbt.com/)
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Discourse Community](https://discourse.getdbt.com/)
- Hands-on practice with real projects
- [dbt Slack Community](https://www.getdbt.com/community/join-the-community/)

---

## HANDS-ON PROJECTS

### Project 1: E-commerce Analytics
**Objective**: Build complete analytics stack for e-commerce store

**Data Sources**:
- Raw orders (transactions)
- Raw customers (user info)
- Raw products (catalog)
- Raw website events (clickstream)

**Deliverables**:
1. Staging models for all sources
2. Intermediate models for:
   - Customer lifetime value
   - Product performance
   - Session analytics
3. Marts:
   - `fct_orders` (order fact table)
   - `dim_customers` (customer dimension)
   - `dim_products` (product dimension)
   - `fct_daily_revenue` (daily aggregates)
4. 20+ tests across all models
5. Complete documentation
6. dbt Cloud job scheduled daily

### Project 2: SaaS Metrics Dashboard
**Objective**: Build MRR, churn, and growth metrics

**Metrics to Calculate**:
- Monthly Recurring Revenue (MRR)
- Customer churn rate
- Revenue churn rate
- Customer Acquisition Cost (CAC)
- Lifetime Value (LTV)
- LTV:CAC ratio
- Net Revenue Retention (NRR)

**Deliverables**:
1. Snapshot of customer subscriptions (SCD Type 2)
2. Incremental event processing
3. Metric calculations as models
4. Metrics defined in dbt Semantic Layer
5. Tests for metric accuracy
6. Documentation with business definitions

### Project 3: Data Quality Framework
**Objective**: Implement comprehensive data quality suite

**Deliverables**:
1. dbt-expectations tests on all models
2. Custom generic tests for business rules
3. Singular tests for reconciliation
4. Monitoring dashboard of test results
5. Alerting on test failures
6. Data quality SLAs documented

### Project 4: Multi-Environment Setup
**Objective**: Set up dev/staging/prod environments

**Deliverables**:
1. Separate profiles for each environment
2. Environment-specific variables
3. CI/CD pipeline with GitHub Actions
4. Slim CI for PR checks
5. Blue/green deployment process
6. Rollback procedures documented

### Project 5: ML Feature Store
**Objective**: Build features for ML model

**Deliverables**:
1. Customer feature table (demographics, behavior)
2. Order feature table (recency, frequency, monetary)
3. Product feature table (popularity, ratings)
4. Time-series aggregations
5. Incremental feature updates
6. Feature documentation
7. Python model for feature validation

---

## ADDITIONAL RESOURCES

### Official Resources
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Learn](https://learn.getdbt.com/)
- [dbt Blog](https://www.getdbt.com/blog/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt GitHub](https://github.com/dbt-labs/dbt-core)

### Community Resources
- [dbt Slack Community](https://www.getdbt.com/community/)
- [Locally Optimistic (newsletter)](https://locallyoptimistic.com/)
- [Analytics Engineer Roundup (newsletter)](https://roundup.getdbt.com/)
- [dbt Package Hub](https://hub.getdbt.com/)

### Best Practices
- Follow dbt Style Guide
- Use consistent naming conventions
- Document everything (models, columns, metrics)
- Test early and often
- Start with staging → intermediate → marts structure
- Use ephemeral for performance
- Incremental for large fact tables
- Version control everything
- Use dbt Cloud or CI/CD for production
- Monitor performance and costs

### Common Pitfalls
- Over-engineering staging models (keep them simple)
- Not using incremental models for large tables
- Forgetting to document
- Not testing enough
- Ignoring performance (large ephemeral models)
- Not using packages (reinventing the wheel)
- Poor folder organization
- Not leveraging dbt features (sources, tests, docs)
- Mixing business logic into staging
- Not using version control properly

---

**Estimated Learning Timeline:**
- **Beginner**: 1-2 weeks (basics, first models, tests)
- **Intermediate**: 3-4 weeks (macros, incremental, snapshots)
- **Advanced**: 6-8 weeks (packages, optimization, CI/CD)
- **Expert**: 6-12 months (custom materializations, complex architectures)

**Total to Proficiency**: 6-12 months with consistent hands-on practice

**Quick Start Path** (for someone with SQL knowledge):
- **Day 1**: Install dbt, initialize project, run first model
- **Day 2-3**: Build staging and marts models
- **Day 4**: Add tests and documentation
- **Day 5**: Deploy to dbt Cloud, schedule job
- **Week 2**: Incremental models and snapshots
- **Week 3-4**: Advanced features (macros, packages, optimization)

Good luck with your dbt journey! 🚀