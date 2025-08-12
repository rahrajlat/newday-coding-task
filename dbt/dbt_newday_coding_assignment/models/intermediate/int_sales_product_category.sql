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
month_name,
SUM(order_amount) as total_revenue,
month_val,
year_num


from 
(
select 
sales_fact.order_amount,
extract(year  from sales_fact.order_date) as year_num,
EXTRACT(MONTH FROM sales_fact.order_date) as month_val,
{{ get_month_name_from_date('sales_fact.order_date') }} AS month_name, --macro for reusability
stg_prd_cat.category_name 
from {{ ref('stg_sales_fact') }} sales_fact
left join product_cte stg_prd
on sales_fact.product_id =stg_prd.product_id 
left join product_category_cte stg_prd_cat
on stg_prd.product_id =stg_prd_cat.product_id 
)
group by category_name,year_num,month_val,month_name
order by category_name,year_num,month_val,month_name

)

--final select

select 
year_num,
category_name,
month_name,
total_revenue




from 

transformation_cte