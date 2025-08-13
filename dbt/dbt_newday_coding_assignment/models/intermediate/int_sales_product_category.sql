/*

## Question 1:
 Create a dbt model that calculates total revenue by product category for each month. Include basic data transformations and aggregations.
*/

with product_cte as 
(

select product_id,
       product_category_id
       from

       {{ ref('stg_product') }}

),

product_category_cte as
(

select product_id,
       category_id,
       category_name
       from

       {{ ref('stg_product_category') }}

),

transformation_cte as 

(


select

category_name,
month_start,
SUM(order_amount) as total_revenue
from 
(
select 
sales_fact.order_amount,
date_trunc('month', sales_fact.order_date)::date AS month_start,
stg_prd_cat.category_name 
from {{ ref('stg_sales_fact') }} sales_fact
left join product_cte stg_prd
on sales_fact.product_id =stg_prd.product_id 
left join product_category_cte stg_prd_cat
on stg_prd.product_id =stg_prd_cat.product_id 
)
group by category_name,month_start
order by category_name,month_start

)

--final select

select 
category_name,
month_start,
total_revenue
from 

transformation_cte