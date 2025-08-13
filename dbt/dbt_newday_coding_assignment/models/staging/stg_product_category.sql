WITH CTE_PRODUCT_CATEGORY AS (

    SELECT * FROM
        {{ source('raw', 'product_category') }}
)

SELECT * FROM CTE_PRODUCT_CATEGORY
