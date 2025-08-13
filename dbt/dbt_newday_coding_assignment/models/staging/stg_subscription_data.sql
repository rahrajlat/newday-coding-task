WITH CTE_SUBSCRIPTION_DATA AS (

    SELECT * FROM
        {{ source('raw', 'subscription_data') }}
)

SELECT * FROM CTE_SUBSCRIPTION_DATA
