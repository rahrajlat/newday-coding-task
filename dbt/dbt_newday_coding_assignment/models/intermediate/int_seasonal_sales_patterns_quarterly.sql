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
        EXTRACT(QUARTER FROM ORDER_DATE) AS QUARTER_NUMBER,
        EXTRACT(YEAR FROM ORDER_DATE) AS YEAR_NO,
        ORDER_AMOUNT

    FROM
        {{ ref('stg_sales_fact') }} AS SF
        LEFT JOIN {{ ref('stg_product') }} AS STG_PRD
            ON SF.PRODUCT_ID = STG_PRD.PRODUCT_ID
        LEFT JOIN {{ ref('stg_product_category') }} AS STG_PRD_CAT
            ON STG_PRD.PRODUCT_ID = STG_PRD_CAT.PRODUCT_ID
    ORDER BY ORDER_DATE
),

SALES_PER_QUARTER AS (

    SELECT
        SALES.*,
        STDDEV_POP(SALES.REVENUE) OVER (PARTITION BY SALES.CATEGORY_NAME) AS STD_DEVIATION,
        LAG(SALES.REVENUE) OVER (PARTITION BY SALES.CATEGORY_NAME) AS PREV_REVENUE
    FROM
        (
            SELECT
                CATEGORY_NAME,
                QUARTER_NUMBER,
                YEAR_NO,
                SUM(ORDER_AMOUNT) AS REVENUE
            FROM
                SALES_JOINED
            GROUP BY CATEGORY_NAME, QUARTER_NUMBER, YEAR_NO
            ORDER BY CATEGORY_NAME, QUARTER_NUMBER, YEAR_NO
        ) AS SALES

)

SELECT
    YEAR_NO,
    CATEGORY_NAME,
    QUARTER_NUMBER,
    REVENUE AS CURRENT_REVENUE,
    PREV_REVENUE,
    CASE
        WHEN PREV_REVENUE IS NULL OR PREV_REVENUE = 0 THEN '1st Quarter'
        ELSE ROUND((REVENUE - PREV_REVENUE) / PREV_REVENUE * 100) || '%'
    END AS QOQ_REV_GROWTH

FROM SALES_PER_QUARTER
ORDER BY YEAR_NO, CATEGORY_NAME, QUARTER_NUMBER
