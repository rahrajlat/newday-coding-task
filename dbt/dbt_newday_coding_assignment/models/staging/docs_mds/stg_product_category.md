{% docs stg_product_category %}

Here's the explanation in the fixed format:

Purpose:
This SQL code is used to create a view of product categories from the raw data.

Logic Flow:
• The SQL code starts by creating a Common Table Expression (CTE) named CTE_PRODUCT_CATEGORY.
• Within the CTE, it selects all columns (*) from the product_category table in the raw schema.
• The source function is used to specify the source of the data as the raw schema and the product_category table.
• Finally, the SQL code selects all columns (*) from the CTE, effectively creating a view of the product categories.
{% enddocs %}

