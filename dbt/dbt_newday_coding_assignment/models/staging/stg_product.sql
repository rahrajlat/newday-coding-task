WITH CTE_PRODUCT AS (

    SELECT * FROM
        {{ source('raw', 'product') }}
)

SELECT * FROM CTE_PRODUCT
