with cte_sales_fact as 

(

    select * from 
 {{ source('raw', 'sales_fact') }}
)


select * from cte_sales_fact