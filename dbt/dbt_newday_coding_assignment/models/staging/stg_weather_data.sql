WITH CTE_WEATHER_DATA AS (

    SELECT * FROM
        {{ source('raw', 'weather_data') }}
)

SELECT * FROM CTE_WEATHER_DATA
