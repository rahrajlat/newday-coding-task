WITH CTE_KAFKA_STREAM AS (

    SELECT * FROM
        {{ source('raw', 'kafka_stream') }}
)

SELECT * FROM CTE_KAFKA_STREAM
