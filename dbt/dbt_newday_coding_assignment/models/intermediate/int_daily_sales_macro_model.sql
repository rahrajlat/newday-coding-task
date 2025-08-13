/*

## Question 7:
Create a dbt macro that accepts date range parameters and filters sales data accordingly.
Use this macro in a model to calculate daily revenue totals. Handle cases where no data exists for the given date range.


*/

{% set start_date = var('start_date') %}
{% set end_date   = var('end_date') %}


SELECT * FROM
    {{ filter_date_range(start_date=start_date,end_date=end_date,table_name=ref('stg_sales_fact'), filter_column='order_date') }}
