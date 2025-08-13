{% docs stg_sales_fact %}

Here is the explanation of the SQL code:

**Purpose:**
Retrieve all rows from the "sales_fact" table in the "dbt_db" database and load them into a new result set.

**Logic Flow:**

• The SQL code uses a Common Table Expression (CTE) named CTE_SALES_FACT to isolate the "sales_fact" table from the "dbt_db" database.
• The SELECT statement within the CTE queries the entire "sales_fact" table and assigns all columns (*) to the CTE result set.
• Finally, the outermost SELECT statement retrieves all rows from the CTE_SALES_FACT result set and returns them as the final output.
{% enddocs %}

