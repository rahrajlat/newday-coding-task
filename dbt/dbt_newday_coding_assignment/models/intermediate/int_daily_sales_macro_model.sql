/*

## Question 7:
Create a dbt macro that accepts date range parameters and filters sales data accordingly.
Use this macro in a model to calculate daily revenue totals. Handle cases where no data exists for the given date range.


*/
{% set start_date = var('start_date') %}
{% set end_date   = var('end_date') %}

WITH DAILY_RECORDS_CTE AS (

    SELECT * FROM {{ filter_date_range(start_date=start_date,end_date=end_date,table_name=ref('stg_sales_fact'), filter_column='order_date') }}),

DAILY_REVENUE_CTE AS (

    SELECT
        SALES_FACT.ORDER_DATE,
        SUM(SALES_FACT.ORDER_AMOUNT) AS TOTAL_REVENUE_BY_ORDERS_AMOUNT,
        SUM(SALES_FACT.ORDER_AMOUNT - (SALES_FACT.ORDER_AMOUNT * COALESCE(SALES_FACT.DISCOUNT_APPLIED, 0))) AS TOTAL_REVENUE_BY_EXCLUDING_SHIPPING,
        SUM(SALES_FACT.ORDER_AMOUNT - (SALES_FACT.ORDER_AMOUNT * COALESCE(SALES_FACT.DISCOUNT_APPLIED, 0)) + COALESCE(SALES_FACT.SHIPPING_COST, 0)) AS TOTAL_REVENUE_BY_INCLUDING_SHIPPING

    FROM DAILY_RECORDS_CTE AS SALES_FACT

    GROUP BY SALES_FACT.ORDER_DATE

)

SELECT
    ORDER_DATE,
    TOTAL_REVENUE_BY_ORDERS_AMOUNT,
    TOTAL_REVENUE_BY_EXCLUDING_SHIPPING,
    TOTAL_REVENUE_BY_INCLUDING_SHIPPING,
    CURRENT_TIMESTAMP AS DBT_INSERT_TIMESTAMP
FROM DAILY_REVENUE_CTE
