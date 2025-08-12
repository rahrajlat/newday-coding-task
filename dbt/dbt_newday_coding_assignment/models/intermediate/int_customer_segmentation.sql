/*


## Question 3: 
Create a dbt model that segments customers into tiers based on their total purchase amount:
- "High Value": Total purchases >= $1000
- "Medium Value": Total purchases between $500-$999
- "Low Value": Total purchases < $500

Include customer names and calculate the number of orders per customer.



*/


with cte_customer as 
(
select customer_id,customer_name from  {{ ref('stg_customer') }}

),

sales_per_customer as 
(


select 
customer_id,
total_orders,
sum(order_amount) as total_purchase_amount


from 

(
select 
customer_id,
count(customer_id) OVER (PARTITION BY customer_id) as total_orders,
order_amount

from {{ ref('stg_sales_fact') }}

)
group by customer_id,total_orders
),


merge_cust_sales
as 

(

select 
spc.customer_id,
cus.customer_name,
total_orders,
total_purchase_amount,
case when total_purchase_amount >= 1000 then 'High Value'
when total_purchase_amount between 500 and 999 then 'Medium Value'
when total_purchase_amount < 500 then 'Low Value'
end customer_tiers

from sales_per_customer spc
left join cte_customer cus
on spc.customer_id=cus.customer_id
)

select * from merge_cust_sales

