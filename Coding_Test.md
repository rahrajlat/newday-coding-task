# DBT NewDay Coding Exercise

## Prerequisites

One of the prerequisites was to get a working DBT environment.

### Actions Taken

- Created a virtual environment and installed the necessary dependencies as listed in the `requirements.txt` file.
- Created a Docker PostgreSQL instance to run and develop locally.  
  *(Setup can be found under `/docker_setup`)*.
- Initialized the database with the following schemas:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS tests;

COMMENT ON SCHEMA raw IS 'Landing/raw data';
COMMENT ON SCHEMA staging IS 'Cleaned/typed staging models';
COMMENT ON SCHEMA intermediate IS 'Transformations between staging and marts';
COMMENT ON SCHEMA marts IS 'Business-facing models';
COMMENT ON SCHEMA tests IS 'Test artifacts';

- The files that was shared was placed under the seeds/ directory and loaded onto the raw schema using the below command

dbt seed --profiles-dir

## DBT Seed Run Log

14:24:34  Running with dbt=1.10.8
14:24:34  Registered adapter: postgres=1.9.0
14:24:34  Found 23 models, 7 seeds, 51 data tests, 15 sources, 437 macros

14:24:34  Concurrency: 1 threads (target=‘dev’)

14:24:34  1 of 7 START seed file raw.customer …………………………………….. [RUN]
14:24:34  1 of 7 OK loaded seed file raw.customer …………………………………. [INSERT 65 in 0.06s]
14:24:34  2 of 7 START seed file raw.kafka_stream …………………………………. [RUN]
14:24:34  2 of 7 OK loaded seed file raw.kafka_stream ……………………………… [INSERT 110 in 0.03s]
14:24:34  3 of 7 START seed file raw.product ……………………………………… [RUN]
14:24:34  3 of 7 OK loaded seed file raw.product ………………………………….. [INSERT 35 in 0.02s]
14:24:34  4 of 7 START seed file raw.product_category ……………………………… [RUN]
14:24:34  4 of 7 OK loaded seed file raw.product_category ………………………….. [INSERT 35 in 0.03s]
14:24:34  5 of 7 START seed file raw.sales_fact …………………………………… [RUN]
14:24:35  5 of 7 OK loaded seed file raw.sales_fact ……………………………….. [INSERT 100 in 0.04s]
14:24:35  6 of 7 START seed file raw.subscription_data …………………………….. [RUN]
14:24:35  6 of 7 OK loaded seed file raw.subscription_data …………………………. [INSERT 100 in 0.03s]
14:24:35  7 of 7 START seed file raw.weather_data …………………………………. [RUN]
14:24:35  7 of 7 OK loaded seed file raw.weather_data ……………………………… [INSERT 144 in 0.03s]

14:24:35  Finished running 7 seeds in 0 hours 0 minutes and 0.40 seconds (0.40s)

14:24:35  Completed successfully

14:24:35  Done. PASS=7 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=7

Ensured the Records are loaded

SELECT COUNT(*) as table_count,'customer' as raw_table_name FROM raw.customer UNION ALL
SELECT COUNT(*) as table_count,'subscription_data' as raw_table_name FROM raw.subscription_data UNION ALL
SELECT COUNT(*) as table_count,'weather_data' as raw_table_name FROM raw.weather_data UNION ALL
SELECT COUNT(*) as table_count,'kafka_stream' as raw_table_name FROM raw.kafka_stream UNION ALL
SELECT COUNT(*) as table_count,'product' as raw_table_name FROM raw.product UNION ALL
SELECT COUNT(*) as table_count,'product_category' as raw_table_name FROM raw.product_category UNION ALL
SELECT COUNT(*) as table_count,'sales_fact' as raw_table_name FROM raw.sales_fact 

|table_count|raw_table_name|
|-----------|--------------|
|65|customer|
|100|subscription_data|
|144|weather_data|
|110|kafka_stream|
|35|product|
|35|product_category|
|100|sales_fact|

## DBT Project Structure

I have structed the DBT project in the below manner,

models
      -staging (One to One copy of the Raw layer)
      -intermediate (Applied the Transformations as per the questions shared)
      -marts (One to One copy of the intermediate)

Below are the models corresponding to the questions

- Question 1 -  models/intermediate/int_sales_payment_method_monthly.sql
- Question 2 -  models/intermediate/int_sales_revenue_product_category_monthly.sql
- Question 3 -  models/intermediate/int_customer_segmentation.sql
- Question 4 -  models/intermediate/int_payment_method_metrics.sql
- Question 5 -  models/intermediate/int_flag_customer_orders.sql
- Question 6 -  models/intermediate/int_seasonal_sales_patterns_monthly.sql
- Question 7 -  models/intermediate/int_daily_sales_macro_model.sql


## DBT Run - Testing

All of the above modes were executed and loaded into the database.

(.venv_dbt) rahulrajasekharan@Rahuls-MacBook-Air dbt_newday_coding_assignment % dbt run --profiles-dir .
14:43:11  Running with dbt=1.10.8
14:43:11  Registered adapter: postgres=1.9.0
14:43:11  Found 23 models, 7 seeds, 51 data tests, 15 sources, 437 macros
14:43:11  
14:43:11  Concurrency: 1 threads (target='dev')
14:43:11  
14:43:11  1 of 23 START sql table model staging.stg_customer ............................. [RUN]
14:43:12  1 of 23 OK created sql table model staging.stg_customer ........................ [SELECT 65 in 0.06s]
14:43:12  2 of 23 START sql table model staging.stg_kafka_stream ......................... [RUN]
14:43:12  2 of 23 OK created sql table model staging.stg_kafka_stream .................... [SELECT 110 in 0.02s]
14:43:12  3 of 23 START sql table model staging.stg_product .............................. [RUN]
14:43:12  3 of 23 OK created sql table model staging.stg_product ......................... [SELECT 35 in 0.02s]
14:43:12  4 of 23 START sql table model staging.stg_product_category ..................... [RUN]
14:43:12  4 of 23 OK created sql table model staging.stg_product_category ................ [SELECT 35 in 0.02s]
14:43:12  5 of 23 START sql table model staging.stg_sales_fact ........................... [RUN]
14:43:12  5 of 23 OK created sql table model staging.stg_sales_fact ...................... [SELECT 100 in 0.02s]
14:43:12  6 of 23 START sql table model staging.stg_subscription_data .................... [RUN]
14:43:12  6 of 23 OK created sql table model staging.stg_subscription_data ............... [SELECT 100 in 0.02s]
14:43:12  7 of 23 START sql table model staging.stg_weather_data ......................... [RUN]
14:43:12  7 of 23 OK created sql table model staging.stg_weather_data .................... [SELECT 144 in 0.02s]
14:43:12  8 of 23 START sql table model intermediate.int_customer_segmentation ........... [RUN]
14:43:12  8 of 23 OK created sql table model intermediate.int_customer_segmentation ...... [SELECT 62 in 0.03s]
14:43:12  9 of 23 START sql table model intermediate.int_daily_sales_macro_model ......... [RUN]
14:43:12  Running Job for the Range 2024-01-15 to 2024-01-16 on the table "dbt_db"."staging"."stg_sales_fact"
14:43:12  Data present for given date range
14:43:12  9 of 23 OK created sql table model intermediate.int_daily_sales_macro_model .... [SELECT 2 in 0.02s]
14:43:12  10 of 23 START sql table model intermediate.int_flag_customer_orders ........... [RUN]
14:43:12  10 of 23 OK created sql table model intermediate.int_flag_customer_orders ...... [SELECT 100 in 0.02s]
14:43:12  11 of 23 START sql table model intermediate.int_payment_method_metrics ......... [RUN]
14:43:12  11 of 23 OK created sql table model intermediate.int_payment_method_metrics .... [SELECT 4 in 0.02s]
14:43:12  12 of 23 START sql table model intermediate.int_sales_payment_method_monthly ... [RUN]
14:43:12  12 of 23 OK created sql table model intermediate.int_sales_payment_method_monthly  [SELECT 99 in 0.02s]
14:43:12  13 of 23 START sql table model intermediate.int_sales_revenue_product_category_monthly  [RUN]
14:43:12  13 of 23 OK created sql table model intermediate.int_sales_revenue_product_category_monthly  [SELECT 35 in 0.02s]
14:43:12  14 of 23 START sql table model intermediate.int_seasonal_sales_patterns_monthly  [RUN]
14:43:12  14 of 23 OK created sql table model intermediate.int_seasonal_sales_patterns_monthly  [SELECT 35 in 0.02s]
14:43:12  15 of 23 START sql table model intermediate.int_seasonal_sales_patterns_quarterly  [RUN]
14:43:12  15 of 23 OK created sql table model intermediate.int_seasonal_sales_patterns_quarterly  [SELECT 15 in 0.02s]
14:43:12  16 of 23 START sql table model marts.mart_customer_segmentation ................ [RUN]
14:43:12  16 of 23 OK created sql table model marts.mart_customer_segmentation ........... [SELECT 62 in 0.02s]
14:43:12  17 of 23 START sql table model marts.mart_daily_sales_macro_model .............. [RUN]
14:43:12  17 of 23 OK created sql table model marts.mart_daily_sales_macro_model ......... [SELECT 2 in 0.02s]
14:43:12  18 of 23 START sql table model marts.mart_flag_customer_orders ................. [RUN]
14:43:12  18 of 23 OK created sql table model marts.mart_flag_customer_orders ............ [SELECT 100 in 0.02s]
14:43:12  19 of 23 START sql table model marts.mart_payment_method_metrics ............... [RUN]
14:43:12  19 of 23 OK created sql table model marts.mart_payment_method_metrics .......... [SELECT 4 in 0.02s]
14:43:12  20 of 23 START sql table model marts.mart_sales_payment_method_monthly ......... [RUN]
14:43:12  20 of 23 OK created sql table model marts.mart_sales_payment_method_monthly .... [SELECT 99 in 0.02s]
14:43:12  21 of 23 START sql table model marts.mart_sales_revenue_product_category_monthly  [RUN]
14:43:12  21 of 23 OK created sql table model marts.mart_sales_revenue_product_category_monthly  [SELECT 35 in 0.02s]
14:43:12  22 of 23 START sql table model marts.mart_seasonal_sales_patterns_monthly ...... [RUN]
14:43:12  22 of 23 OK created sql table model marts.mart_seasonal_sales_patterns_monthly . [SELECT 35 in 0.02s]
14:43:12  23 of 23 START sql table model marts.mart_seasonal_sales_patterns_quarterly .... [RUN]
14:43:13  23 of 23 OK created sql table model marts.mart_seasonal_sales_patterns_quarterly  [SELECT 15 in 0.02s]
14:43:13  
14:43:13  Finished running 23 table models in 0 hours 0 minutes and 1.34 seconds (1.34s).
14:43:13  
14:43:13  Completed successfully
14:43:13  
14:43:13  Done. PASS=23 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=23


Count Checks ( Staging )

SELECT COUNT(*) as table_count,'stg_customer' as staging_table_name FROM staging.stg_customer UNION ALL
SELECT COUNT(*) as table_count,'stg_kafka_stream' as staging_table_name FROM staging.stg_kafka_stream UNION ALL
SELECT COUNT(*) as table_count,'stg_product' as staging_table_name FROM staging.stg_product UNION ALL
SELECT COUNT(*) as table_count,'stg_product_category' as staging_table_name FROM staging.stg_product_category UNION ALL
SELECT COUNT(*) as table_count,'stg_sales_fact' as staging_table_name FROM staging.stg_sales_fact UNION ALL
SELECT COUNT(*) as table_count,'stg_subscription_data' as staging_table_name FROM staging.stg_subscription_data UNION ALL
SELECT COUNT(*) as table_count,'stg_weather_data' as staging_table_name FROM staging.stg_weather_data 

|table_count|staging_table_name|
|-----------|------------------|
|65|stg_customer|
|110|stg_kafka_stream|
|35|stg_product|
|35|stg_product_category|
|100|stg_sales_fact|
|100|stg_subscription_data|
|144|stg_weather_data|


Count Checks ( Intermediate )

SELECT COUNT(*) as table_count,'int_daily_sales_macro_model' as int_table FROM intermediate.int_daily_sales_macro_model UNION ALL
SELECT COUNT(*) as table_count,'int_flag_customer_orders' as int_table FROM intermediate.int_flag_customer_orders UNION ALL
SELECT COUNT(*) as table_count,'int_payment_method_metrics' as int_table FROM intermediate.int_payment_method_metrics UNION ALL
SELECT COUNT(*) as table_count,'int_sales_payment_method_monthly' as int_table FROM intermediate.int_sales_payment_method_monthly UNION ALL
SELECT COUNT(*) as table_count,'int_sales_revenue_product_category_monthly' as int_table FROM intermediate.int_sales_revenue_product_category_monthly UNION ALL
SELECT COUNT(*) as table_count,'int_seasonal_sales_patterns_monthly' as int_table FROM intermediate.int_seasonal_sales_patterns_monthly UNION ALL
SELECT COUNT(*) as table_count,'int_customer_segmentation' as int_table FROM intermediate.int_customer_segmentation UNION ALL
SELECT COUNT(*) as table_count,'int_seasonal_sales_patterns_quarterly' as int_table FROM intermediate.int_seasonal_sales_patterns_quarterly 


|table_count|int_table|
|-----------|---------|
|2|int_daily_sales_macro_model|
|100|int_flag_customer_orders|
|4|int_payment_method_metrics|
|99|int_sales_payment_method_monthly|
|35|int_sales_revenue_product_category_monthly|
|35|int_seasonal_sales_patterns_monthly|
|62|int_customer_segmentation|
|15|int_seasonal_sales_patterns_quarterly|

Count Checks ( Marts )

SELECT COUNT(*) as table_count,'mart_customer_segmentation' as mart_table FROM marts.mart_customer_segmentation UNION ALL
SELECT COUNT(*) as table_count,'mart_seasonal_sales_patterns_quarterly' as mart_table FROM marts.mart_seasonal_sales_patterns_quarterly UNION ALL
SELECT COUNT(*) as table_count,'mart_daily_sales_macro_model' as mart_table FROM marts.mart_daily_sales_macro_model UNION ALL
SELECT COUNT(*) as table_count,'mart_flag_customer_orders' as mart_table FROM marts.mart_flag_customer_orders UNION ALL
SELECT COUNT(*) as table_count,'mart_payment_method_metrics' as mart_table FROM marts.mart_payment_method_metrics UNION ALL
SELECT COUNT(*) as table_count,'mart_sales_payment_method_monthly' as mart_table FROM marts.mart_sales_payment_method_monthly UNION ALL
SELECT COUNT(*) as table_count,'mart_sales_revenue_product_category_monthly' as mart_table FROM marts.mart_sales_revenue_product_category_monthly UNION ALL
SELECT COUNT(*) as table_count,'mart_seasonal_sales_patterns_monthly' as mart_table FROM marts.mart_seasonal_sales_patterns_monthly


|table_count|mart_table|
|-----------|----------|
|62|mart_customer_segmentation|
|15|mart_seasonal_sales_patterns_quarterly|
|2|mart_daily_sales_macro_model|
|100|mart_flag_customer_orders|
|4|mart_payment_method_metrics|
|99|mart_sales_payment_method_monthly|
|35|mart_sales_revenue_product_category_monthly|
|35|mart_seasonal_sales_patterns_monthly|



## DBT Tests -

I have added tests for the marts tables and they can be found models/marts/mart_tables.yml

## DBT Document Automation 

- This was not a part of the test requirement , but I would like share an idea of automating the DBT document process using a local large language model. I discussed briefly during my 1st round of interview , just added that as an innovation.

The script is placed under dbt/dbt_newday_coding_assignment/llm_doc_generator , which then generates the documentation in the respective folder structures.

staging - dbt_newday_coding_assignment/models/staging/docs_mds
intermediate - dbt_newday_coding_assignment/models/intermediate/docs_mds

When the dbt docs generate command is executed , this gets picked up and shown in the web interface.

## Code Quality - SQL Fluff implementation

As part of the code quality, I have configured sqlfull within the dbt project. The configuration file is under 

dbt/dbt_newday_coding_assignment/.sqlfluff

Which fixes the SQL based on the rules defined,

sqlfluff fix --templater dbt /Users/rahulrajasekharan/vscode_proj/newday-dbt/newday-coding-task/dbt/dbt_newday_coding_assignment/.sqlfluff models/intermediate/



