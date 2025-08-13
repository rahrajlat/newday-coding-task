/*

## Question 2: 
Extend the previous model to handle edge cases where `order_quantity` is zero and calculate the percentage of sales coming from each payment method. Handle null values appropriately.

*/



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
revenue_monthly_pm,
revenue_monthly,
payment_method
from 
(
select 
sales_fact.order_amount,
sum(sales_fact.order_amount) over (partition by stg_prd_cat.category_name,date_trunc('month', sales_fact.order_date)::date ) as revenue_monthly,
sum(sales_fact.order_amount) over (partition by stg_prd_cat.category_name,payment_method,date_trunc('month', sales_fact.order_date)::date ) as revenue_monthly_pm,
payment_method,
date_trunc('month', sales_fact.order_date)::date AS month_start,
stg_prd_cat.category_name 
from {{ ref('stg_sales_fact') }} sales_fact
left join product_cte stg_prd
on sales_fact.product_id =stg_prd.product_id 
left join product_category_cte stg_prd_cat
on stg_prd.product_id =stg_prd_cat.product_id 
where order_quantity > 0
)
)

--final select

select 
category_name,
month_start,
revenue_monthly_pm,
payment_method,
revenue_monthly,
revenue_monthly_pm /revenue_monthly * 100 as revenue_method_pct
from 

transformation_cte







