/*

## Question 1:
 Create a dbt model that calculates total revenue by product category for each month. Include basic data transformations and aggregations.

 Rahul - As the question has revenue , I had a doubt as this needs to be just based on the Order Amount or to include the discounts & shipping cost. But I have built the model
 considering all the below 3.

1) Revenue puerely based on Order Amount
2) Revenue with just discounts applied.
3) Revenue with both discounts/shipping costs applied.

*/


--Product Category cte to bring in the category_name
WITH PRODUCT_CATEGORY_CTE AS (

    SELECT
        PRODUCT_ID,
        CATEGORY_ID,
        CATEGORY_NAME
    FROM

        {{ ref('stg_product_category') }}

),

--Join and apply aggregations
TRANSFORMATION_CTE AS (

    SELECT

        CATEGORY_NAME,
        MONTH_START AS MONTHLY_DATE,
        SUM(ORDER_AMOUNT) AS TOTAL_REVENUE_ONLY_ORDER_AMOUNT,
        SUM(NET_REVENUE_EXCL_SHIPPING) AS REVENUE_EXCL_SHIPPING,
        SUM(NET_REVENUE_INCL_SHIPPING) AS REVENUE_INCL_SHIPPING
    FROM
        (
            SELECT
                SALES_FACT.ORDER_AMOUNT,
                DATE_TRUNC('month', SALES_FACT.ORDER_DATE)::DATE AS MONTH_START,
                STG_PRD_CAT.CATEGORY_NAME,
                ORDER_AMOUNT - (ORDER_AMOUNT * COALESCE(DISCOUNT_APPLIED, 0)) AS NET_REVENUE_EXCL_SHIPPING,
                ORDER_AMOUNT - (ORDER_AMOUNT * COALESCE(DISCOUNT_APPLIED, 0)) + COALESCE(SHIPPING_COST, 0) AS NET_REVENUE_INCL_SHIPPING
            FROM {{ ref('stg_sales_fact') }} AS SALES_FACT
                LEFT JOIN PRODUCT_CATEGORY_CTE AS STG_PRD_CAT
                    ON SALES_FACT.PRODUCT_ID = STG_PRD_CAT.PRODUCT_ID
        )
    GROUP BY CATEGORY_NAME, MONTH_START
    ORDER BY CATEGORY_NAME, MONTH_START

)

--final select

SELECT
    CATEGORY_NAME,
    MONTHLY_DATE,
    TOTAL_REVENUE_ONLY_ORDER_AMOUNT,
    REVENUE_EXCL_SHIPPING,
    REVENUE_INCL_SHIPPING,
    CURRENT_TIMESTAMP AS DBT_INSERT_TIMESTAMP

FROM

    TRANSFORMATION_CTE
