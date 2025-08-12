with cte_product as 

(

    select * from 
 {{ source('raw', 'product') }}
)


select * from cte_product