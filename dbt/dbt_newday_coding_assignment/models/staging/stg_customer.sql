WITH CTE_CUSTOMER AS (

    SELECT * FROM
        {{ source('raw', 'customer') }}
)

SELECT * FROM CTE_CUSTOMER
