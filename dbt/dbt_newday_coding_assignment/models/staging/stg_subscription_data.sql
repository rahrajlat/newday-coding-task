with cte_subscription_data as 

(

    select * from 
 {{ source('raw', 'subscription_data') }}
)


select * from cte_subscription_data