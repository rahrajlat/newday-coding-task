/*

## Question 1:
 Create a dbt model that calculates total revenue by product category for each month. Include basic data transformations and aggregations.

 Rahul - As the question has revenue , I had a doubt as this needs to be just based on the Order Amount or to include the discounts & shipping cost. But I have built the model
 considering all the below 3.

1) Revenue puerely based on Order Amount
2) Revenue with just discounts applied.
3) Revenue with both discounts/shipping costs applied.

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
        MONTH_START AS MONTHLY_DATE,
        SUM(ORDER_AMOUNT) AS TOTAL_REVENUE_ORDERS,
        SUM(net_revenue_excl_shipping) as net_revenue_excl_shipping,
        SUM(net_revenue_incl_shipping) as net_revenue_incl_shipping
    FROM
        (
            SELECT
                SALES_FACT.ORDER_AMOUNT,
                DATE_TRUNC('month', SALES_FACT.ORDER_DATE)::DATE AS MONTH_START,
                STG_PRD_CAT.CATEGORY_NAME,
                order_amount - (order_amount * COALESCE(discount_applied, 0)) as net_revenue_excl_shipping,
                order_amount - (order_amount * COALESCE(discount_applied, 0)) + COALESCE(shipping_cost, 0) as net_revenue_incl_shipping
            FROM {{ ref('stg_sales_fact') }} AS SALES_FACT
                LEFT JOIN PRODUCT_CTE AS STG_PRD
                    ON SALES_FACT.PRODUCT_ID = STG_PRD.PRODUCT_ID
                LEFT JOIN PRODUCT_CATEGORY_CTE AS STG_PRD_CAT
                    ON STG_PRD.PRODUCT_ID = STG_PRD_CAT.PRODUCT_ID
        )
    GROUP BY CATEGORY_NAME, MONTH_START
    ORDER BY CATEGORY_NAME, MONTH_START

)

--final select

SELECT
    CATEGORY_NAME,
    MONTHLY_DATE,
    TOTAL_REVENUE_ORDERS,
    net_revenue_excl_shipping,
    net_revenue_incl_shipping

FROM

    TRANSFORMATION_CTE
