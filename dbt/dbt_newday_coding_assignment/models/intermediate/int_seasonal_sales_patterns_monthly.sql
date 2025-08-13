/*


## Question 6:
Analyze seasonal sales patterns by creating a model that shows:
- Monthly sales trends by product category
- Quarter-over-quarter growth rates
- Identify the best and worst performing months for each category
- Calculate the coefficient of variation to measure sales volatility


*/

WITH SALES_JOINED AS (
    SELECT
        ORDER_DATE,
        STG_PRD_CAT.CATEGORY_NAME,
        DATE_TRUNC('month', ORDER_DATE)::DATE AS MONTHLY_DATE,
        ORDER_AMOUNT

    FROM
        {{ ref('stg_sales_fact') }} AS SF
        LEFT JOIN {{ ref('stg_product') }} AS STG_PRD
            ON SF.PRODUCT_ID = STG_PRD.PRODUCT_ID
        LEFT JOIN {{ ref('stg_product_category') }} AS STG_PRD_CAT
            ON STG_PRD.PRODUCT_ID = STG_PRD_CAT.PRODUCT_ID
),

SALES_PER_MONTH AS (

    SELECT
        SALES.*,
        ROW_NUMBER() OVER (PARTITION BY SALES.CATEGORY_NAME ORDER BY SALES.REVENUE DESC) AS BEST_ROW_RN,
        STDDEV_POP(SALES.REVENUE) OVER (PARTITION BY SALES.CATEGORY_NAME) AS STD_DEVIATION
    FROM
        (
            SELECT
                CATEGORY_NAME,
                MONTHLY_DATE,
                SUM(ORDER_AMOUNT) AS REVENUE
            FROM
                SALES_JOINED
            GROUP BY CATEGORY_NAME, MONTHLY_DATE
            ORDER BY CATEGORY_NAME, MONTHLY_DATE
        ) AS SALES

)

SELECT
    CATEGORY_NAME,
    MONTHLY_DATE,
    REVENUE,
    CASE
        WHEN BEST_ROW_RN = MIN(BEST_ROW_RN) OVER (PARTITION BY CATEGORY_NAME) THEN 'BEST'
        WHEN BEST_ROW_RN = MAX(BEST_ROW_RN) OVER (PARTITION BY CATEGORY_NAME) THEN 'WORST'
    END AS BEST_WORST_MONTH,
    STDDEV_POP(REVENUE) OVER (PARTITION BY CATEGORY_NAME) / AVG(REVENUE) OVER (PARTITION BY CATEGORY_NAME) AS CO_EFFICIENT_OF_VARIATION

FROM SALES_PER_MONTH
ORDER BY CATEGORY_NAME, MONTHLY_DATE
