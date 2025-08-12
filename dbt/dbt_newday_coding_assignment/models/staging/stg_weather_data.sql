with cte_weather_data as 

(

    select * from 
 {{ source('raw', 'weather_data') }}
)


select * from cte_weather_data