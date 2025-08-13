{% docs stg_customer %}

Here is the explanation of the given SQL code:

Purpose:
This SQL code is retrieving data from a raw customer table and presenting it in its original form.

Logic Flow:
• The code starts by defining a Common Table Expression (CTE) named CTE_CUSTOMER.
• The CTE queries the customer table from the raw source, using the {{ source('raw', 'customer') }} syntax.
• The query selects all columns (SELECT *) from the customer table in the raw source.
• Finally, the code selects all columns (SELECT *) from the CTE_CUSTOMER CTE.
{% enddocs %}

