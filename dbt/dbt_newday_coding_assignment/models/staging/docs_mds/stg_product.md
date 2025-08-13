{% docs stg_product %}

Here is the explanation:

Purpose:
This SQL code defines a Common Table Expression (CTE) and then selects all data from it, effectively copying the raw product table into a temporary view.

Logic Flow:
• The SQL code starts by defining a CTE named "CTE_PRODUCT" that selects all columns (*) from the "product" table in the "raw" schema of the "dbt_db" database.
• This CTE is then used as the source for a subsequent SELECT statement, which also selects all columns (*).
• The result is essentially a temporary copy of the original product table.
{% enddocs %}

