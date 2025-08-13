/*

## Question 4:
Create a model that analyzes payment method preferences by calculating:
- Total revenue by payment method
- Average order value by payment method
- Number of orders by payment method
- Percentage distribution of each payment method


*/

WITH PAYMENT_REVENUE AS (

    SELECT

        SUM(SALES_FACT.ORDER_AMOUNT) AS TOTAL_REVENUE_BY_ORDERS_AMOUNT,
        SUM(SALES_FACT.order_amount - (SALES_FACT.order_amount * COALESCE(SALES_FACT.discount_applied, 0))) AS TOTAL_REVENUE_BY_EXCLUDING_SHIPPING,
        SUM(SALES_FACT.order_amount - (SALES_FACT.order_amount * COALESCE(SALES_FACT.discount_applied, 0)) + COALESCE(SALES_FACT.shipping_cost, 0)) AS TOTAL_REVENUE_BY_INCLUDING_SHIPPING,
        PAYMENT_METHOD

    FROM

        {{ ref('stg_sales_fact') }} SALES_FACT

    GROUP BY PAYMENT_METHOD

),

AVG_PAYMENT_ORDER_VALUE AS (

    SELECT

        AVG(ORDER_AMOUNT) AS AVG_ORDER_VALUE,
        PAYMENT_METHOD

    FROM

        {{ ref('stg_sales_fact') }}

    GROUP BY PAYMENT_METHOD
)

,

NO_OF_PAYMENT_ORDERS AS (

    SELECT

        COUNT(ORDER_ID) AS NUM_OF_ORDERS,
        PAYMENT_METHOD

    FROM

        {{ ref('stg_sales_fact') }}

    GROUP BY PAYMENT_METHOD
),

TRANSFORMATION_CTE AS (
    SELECT
        PR.PAYMENT_METHOD,
        TOTAL_REVENUE_BY_ORDERS_AMOUNT,
        TOTAL_REVENUE_BY_EXCLUDING_SHIPPING,
        TOTAL_REVENUE_BY_INCLUDING_SHIPPING,
        AVG_ORDER_VALUE,

        NUM_OF_ORDERS,

        ROUND((NUM_OF_ORDERS::NUMERIC / SUM(NUM_OF_ORDERS) OVER ()) * 100, 2) || '%'
            AS PERCENTAGE_DISTRIBUTION_NUM

    FROM PAYMENT_REVENUE AS PR
        INNER JOIN AVG_PAYMENT_ORDER_VALUE AS APOV
            ON PR.PAYMENT_METHOD = APOV.PAYMENT_METHOD

        INNER JOIN NO_OF_PAYMENT_ORDERS AS NOPO
            ON PR.PAYMENT_METHOD = NOPO.PAYMENT_METHOD

)

SELECT * FROM TRANSFORMATION_CTE
