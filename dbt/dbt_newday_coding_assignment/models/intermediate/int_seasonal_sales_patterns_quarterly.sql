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
EXTRACT(QUARTER FROM order_date) as quarter_number,
EXTRACT(YEAR FROM order_date) as year_no,
order_amount

from 
{{ ref('stg_sales_fact') }} sf 
left join {{ ref('stg_product') }}stg_prd
on sf.product_id=stg_prd.product_id
left join {{ ref('stg_product_category') }} stg_prd_cat 
on stg_prd.product_id=stg_prd_cat.product_id
order by order_date
),

sales_per_quarter as 

(


select sales.* ,
STDDEV_POP(revenue) OVER (PARTITION BY category_name) as std_deviation,
LAG(revenue) OVER(partition by category_name ) as prev_revenue
from 
(
select 
category_name,
quarter_number,
year_no,
sum(order_amount) as revenue
from 
sales_joined
group by category_name,quarter_number,year_no
order by category_name,quarter_number,year_no
) sales

)

select 
year_no,
category_name,
quarter_number,
revenue as current_revenue,
prev_revenue,
CASE WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN '1st Quarter'
       ELSE ROUND((revenue - prev_revenue) / prev_revenue * 100) || '%' END AS qoq_rev_growth


from sales_per_quarter
order by year_no,category_name,quarter_number







