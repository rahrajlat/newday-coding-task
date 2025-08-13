/*


## Question 3:
Create a dbt model that segments customers into tiers based on their total purchase amount:
- "High Value": Total purchases >= $1000
- "Medium Value": Total purchases between $500-$999
- "Low Value": Total purchases < $500

Include customer names and calculate the number of orders per customer.

Rahul - Looking at the data there are some customers who have no transaction in the fact table , show we include them ? - I have included it by doing a Right join
and  COALESCE

*/


WITH CTE_CUSTOMER AS (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME
    FROM {{ ref('stg_customer') }}
    WHERE
    STATUS= 'active'

),

SALES_PER_CUSTOMER AS (

    SELECT
        CUSTOMER_ID,
        TOTAL_ORDERS,
        SUM(ORDER_AMOUNT) AS TOTAL_PURCHASE_AMOUNT

    FROM

        (
            SELECT
                CUSTOMER_ID,
                COUNT(CUSTOMER_ID) OVER (PARTITION BY CUSTOMER_ID) AS TOTAL_ORDERS,
                ORDER_AMOUNT

            FROM {{ ref('stg_sales_fact') }}

        )
    GROUP BY CUSTOMER_ID, TOTAL_ORDERS
),

MERGE_CUST_SALES AS (

    SELECT
         COALESCE(SPC.CUSTOMER_ID, CUS.CUSTOMER_ID) AS CUSTOMER_ID,
    CUS.CUSTOMER_NAME,
    COALESCE(TOTAL_ORDERS, 0) AS TOTAL_ORDERS,
    COALESCE(TOTAL_PURCHASE_AMOUNT, 0) AS TOTAL_PURCHASE_AMOUNT,
        CASE
            WHEN TOTAL_PURCHASE_AMOUNT >= 1000 THEN 'High Value'
            WHEN TOTAL_PURCHASE_AMOUNT BETWEEN 500 AND 999 THEN 'Medium Value'
            WHEN TOTAL_PURCHASE_AMOUNT < 500 THEN 'Low Value'
            WHEN TOTAL_PURCHASE_AMOUNT IS NULL THEN 'Low Value'
        END AS CUSTOMER_TIERS

    FROM SALES_PER_CUSTOMER AS SPC
        RIGHT JOIN CTE_CUSTOMER AS CUS -- There can be customers who havent placed an Order?
            ON SPC.CUSTOMER_ID = CUS.CUSTOMER_ID 
)

SELECT * FROM MERGE_CUST_SALES
