{% docs stg_subscription_data %}

Here is the explanation of the SQL code:

Purpose:
This SQL code retrieves data from a raw subscription data table and makes it available for querying.

Logic Flow:
• The code starts by creating a Common Table Expression (CTE) named CTE_SUBSCRIPTION_DATA.
• Within the CTE, the code selects all columns (*) from a specific table named subscription_data in the raw schema of the database.
• The CTE is then used as the source for another SQL query that also selects all columns (*) from the CTE.
{% enddocs %}

