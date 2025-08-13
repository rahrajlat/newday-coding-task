/*

## Question 5:
Create a dbt model that flags orders for review based on business rules:
- `discount_applied` > 30%
- `shipping_cost` > 10% of `order_amount`
- Handle null values in both `discount_applied` and `shipping_cost`

*/


WITH SALES_FACT_CTE AS (
    SELECT

        ORDER_ID,
        ROUND(COALESCE(DISCOUNT_APPLIED, 0) * 100) AS DISCOUNT_APPLIED,
        COALESCE(SHIPPING_COST, 0) AS SHIPPING_COST,
        ORDER_AMOUNT,
        ORDER_AMOUNT * 10 / 100 AS FLAG_PERCENTAGE
    FROM

        {{ ref('stg_sales_fact') }}

),

TRANSFORMATION_CTE AS (
    SELECT
        ORDER_ID,
        DISCOUNT_APPLIED,
        SHIPPING_COST,
        ORDER_AMOUNT,
        FLAG_PERCENTAGE,
        CASE
            WHEN DISCOUNT_APPLIED > 30 OR SHIPPING_COST > FLAG_PERCENTAGE
                THEN
                    'Y'
            ELSE
                'N'
        END AS FLAGGED_TRANSACTION,
        CONCAT_WS(
            '; ',
            CASE WHEN DISCOUNT_APPLIED > 30 THEN 'DISCOUNT>30%' END,
            CASE WHEN SHIPPING_COST > FLAG_PERCENTAGE THEN 'SHIPPING>10%' END
        ) AS FLAG_REASONS

    FROM SALES_FACT_CTE

)

SELECT * FROM TRANSFORMATION_CTE
