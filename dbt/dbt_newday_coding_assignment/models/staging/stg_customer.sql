with cte_customer as 

(

    select * from 
 {{ source('raw', 'customer') }}
)


select * from cte_customer