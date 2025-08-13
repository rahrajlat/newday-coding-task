WITH CTE_SALES_FACT AS (

    SELECT * FROM
        {{ source('raw', 'sales_fact') }}
)

SELECT * FROM CTE_SALES_FACT
