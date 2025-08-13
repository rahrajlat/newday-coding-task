{% docs stg_customer %}

Here is the explanation of the SQL code:

Purpose:
This SQL code creates a temporary view of customer data from a raw database table.

Logic Flow:
• The code defines a Common Table Expression (CTE) named CTE_CUSTOMER that selects all columns (*) from a table called "customer" in the "raw" schema of the "dbt_db" database.
• The CTE is then used as the source for the main query, which also selects all columns (*) from the CTE.
• This effectively creates a temporary view of the customer data that can be used for further processing or analysis.
{% enddocs %}

