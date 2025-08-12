with cte_kafka_stream as 

(

    select * from 
 {{ source('raw', 'kafka_stream') }}
)


select * from cte_kafka_stream