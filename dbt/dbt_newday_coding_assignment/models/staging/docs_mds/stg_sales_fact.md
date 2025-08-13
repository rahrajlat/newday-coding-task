{% docs stg_sales_fact %}

Here is the explanation of the SQL code:

**Purpose:**
This SQL code retrieves and combines sales data from a raw table in a database.

**Logic Flow:**

* The code starts by creating a temporary result set (CTE) named CTE_SALES_FACT that selects all columns (*) from a specific source table {{ source('raw', 'sales_fact') }}.
* This source table is located in the "raw" schema and is named "sales_fact".
* The CTE is then used as the basis for a subsequent query, which simply selects all columns (*) from the CTE.
* The result is a dataset that contains sales data.
{% enddocs %}

