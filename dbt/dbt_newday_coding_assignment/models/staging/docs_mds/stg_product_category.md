{% docs stg_product_category %}

Here is the explanation of the SQL code:

Purpose:
The SQL code retrieves data from a raw table named "product_category" and creates a temporary view for further analysis.

Logic Flow:
• The query starts by creating a Common Table Expression (CTE) named "CTE_PRODUCT_CATEGORY".
• Within the CTE, it selects all columns (*) from a source table "product_category" in the "raw" schema.
• The CTE is then used as the basis for a SELECT statement that retrieves all rows and columns from the CTE.
{% enddocs %}

