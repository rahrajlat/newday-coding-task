{% docs stg_subscription_data %}

Here is the explanation of the given SQL code:

**Purpose:**
This SQL code extracts data from a raw table named "subscription_data" and makes it available for further processing.

**Logic Flow:**

* It uses a Common Table Expression (CTE) to select all columns (SELECT *) from the "raw" schema in the "dbt_db" database, where the table is named "subscription_data".
* The CTE is then referred to as CTE_SUBSCRIPTION_DATA.
* The final SELECT statement simply selects all columns (SELECT *) from this CTE.
{% enddocs %}

