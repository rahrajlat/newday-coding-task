/*

## Question 5: 
Create a dbt model that flags orders for review based on business rules:
- `discount_applied` > 30%
- `shipping_cost` > 10% of `order_amount`
- Handle null values in both `discount_applied` and `shipping_cost`

*/


with sales_fact_cte as 
(
select 

order_id,
round(COALESCE(discount_applied, 0) * 100) as discount_applied,
COALESCE(shipping_cost, 0) as shipping_cost,
order_amount,
order_amount * 10/100 as flag_percentage
from 

{{ ref('stg_sales_fact') }}

),

transformation_cte as 
(
select 
order_id,
discount_applied,
shipping_cost,
order_amount,
flag_percentage,
case when discount_applied > 30 or shipping_cost > flag_percentage then
'Y'
else
'N'
end as flagged_transaction,
concat_ws(
        '; ',
        case when discount_applied > 30 then 'DISCOUNT>30%' end,
        case when shipping_cost > flag_percentage then 'SHIPPING>10%' end
    ) as flag_reasons

from sales_fact_cte





)

select * from transformation_cte


