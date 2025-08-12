{% set start_date = var('start_date') %}
{% set end_date   = var('end_date') %}


select * from 
{{ filter_date_range(start_date=start_date,end_date=end_date,table_name=ref('stg_sales_fact'), filter_column='order_date') }}

