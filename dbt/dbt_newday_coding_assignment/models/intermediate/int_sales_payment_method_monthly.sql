/*

## Question 1:
 Create a dbt model that calculates total revenue by product category for each month. Include basic data transformations and aggregations.
*/


/*

## Question 2:
Extend the previous model to handle edge cases where `order_quantity` is zero 
and calculate the percentage of sales coming from each payment method. Handle null values appropriately.

*/




WITH PRODUCT_CTE AS (

    SELECT
        PRODUCT_ID,
        PRODUCT_CATEGORY_ID
    FROM

        {{ ref('stg_product') }}

),

PRODUCT_CATEGORY_CTE AS (

    SELECT
        PRODUCT_ID,
        CATEGORY_ID,
        CATEGORY_NAME
    FROM

        {{ ref('stg_product_category') }}

),

TRANSFORMATION_CTE AS (

    SELECT

        CATEGORY_NAME,
        MONTH_START,
        REVENUE_MONTHLY_PM,
        REVENUE_MONTHLY,
        PAYMENT_METHOD,
        REVENUE_MONTHLY_PM / REVENUE_MONTHLY * 100 AS REVENUE_METHOD_PCT
    FROM
        (
            SELECT
                SALES_FACT.ORDER_AMOUNT,
                SUM(SALES_FACT.ORDER_AMOUNT) OVER (PARTITION BY STG_PRD_CAT.CATEGORY_NAME, DATE_TRUNC('month', SALES_FACT.ORDER_DATE)::DATE) AS REVENUE_MONTHLY,
                SUM(SALES_FACT.ORDER_AMOUNT) OVER (PARTITION BY STG_PRD_CAT.CATEGORY_NAME, PAYMENT_METHOD, DATE_TRUNC('month', SALES_FACT.ORDER_DATE)::DATE) AS REVENUE_MONTHLY_PM,
                PAYMENT_METHOD,
                DATE_TRUNC('month', SALES_FACT.ORDER_DATE)::DATE AS MONTH_START,
                STG_PRD_CAT.CATEGORY_NAME
            FROM {{ ref('stg_sales_fact') }} AS SALES_FACT
                LEFT JOIN PRODUCT_CTE AS STG_PRD
                    ON SALES_FACT.PRODUCT_ID = STG_PRD.PRODUCT_ID
                LEFT JOIN PRODUCT_CATEGORY_CTE AS STG_PRD_CAT
                    ON STG_PRD.PRODUCT_ID = STG_PRD_CAT.PRODUCT_ID
            WHERE ORDER_QUANTITY > 0
        )
)

--final select

SELECT
    CATEGORY_NAME,
    MONTH_START,
    REVENUE_MONTHLY_PM,
    PAYMENT_METHOD,
    REVENUE_MONTHLY,
    REVENUE_METHOD_PCT
    
FROM

    TRANSFORMATION_CTE
