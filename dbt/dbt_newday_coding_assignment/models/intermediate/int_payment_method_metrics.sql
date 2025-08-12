/*

## Question 4: 
Create a model that analyzes payment method preferences by calculating:
- Total revenue by payment method
- Average order value by payment method
- Number of orders by payment method
- Percentage distribution of each payment method


*/

with payment_revenue as 

(

select 

sum(order_amount) as total_revenue,
payment_method

from 

{{ ref('stg_sales_fact') }}

group by payment_method

),

avg_payment_order_value as 

(

select 

avg(order_amount) as avg_revenue,
payment_method

from 

{{ ref('stg_sales_fact') }}

group by payment_method
)

,

no_of_payment_orders as 

(

select 

count(order_id) as num_of_orders,
payment_method

from 

{{ ref('stg_sales_fact') }}

group by payment_method
),

transformation_cte as 
(
select 
pr.payment_method,
total_revenue,
avg_revenue,
num_of_orders,

round((num_of_orders::numeric / sum(num_of_orders) over()) * 100, 2) || '%'
  AS percentage_distribution_num

from payment_revenue pr 
inner join avg_payment_order_value apov
on pr.payment_method=apov.payment_method

inner join no_of_payment_orders nopo
on pr.payment_method=nopo.payment_method

)

select * from transformation_cte








