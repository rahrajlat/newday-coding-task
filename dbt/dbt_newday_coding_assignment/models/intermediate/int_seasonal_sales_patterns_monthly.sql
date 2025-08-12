/*


## Question 6:
Analyze seasonal sales patterns by creating a model that shows:
- Monthly sales trends by product category
- Quarter-over-quarter growth rates
- Identify the best and worst performing months for each category
- Calculate the coefficient of variation to measure sales volatility


*/

with sales_joined
as 


(
select 
order_date,
stg_prd_cat.category_name,
date_trunc('month', order_date)::date as monthly_date,
order_amount

from 
{{ ref('stg_sales_fact') }} sf 
left join {{ ref('stg_product') }}stg_prd
on sf.product_id=stg_prd.product_id
left join {{ ref('stg_product_category') }} stg_prd_cat 
on stg_prd.product_id=stg_prd_cat.product_id
),

sales_per_month as 

(


select sales.* ,
row_number () over (partition by category_name order by revenue desc) as best_row_rn,
STDDEV_POP(revenue) OVER (PARTITION BY category_name) as std_deviation
from 
(
select 
category_name,
monthly_date,
sum(order_amount) as revenue
from 
sales_joined
group by category_name,monthly_date
order by category_name,monthly_date
) sales

)

select 
category_name,
monthly_date,
revenue,
case when best_row_rn = min(best_row_rn) over(partition by category_name) then 'BEST'
when best_row_rn = max(best_row_rn) over(partition by category_name) then 'WORST'
else null 
end as best_worst_month,
STDDEV_POP(revenue) OVER (PARTITION BY category_name) / AVG(revenue) OVER (PARTITION BY category_name) as co_efficient_of_variation


from sales_per_month
order by category_name,monthly_date





