with cte_product_category as 

(

    select * from 
 {{ source('raw', 'product_category') }}
)


select * from cte_product_category