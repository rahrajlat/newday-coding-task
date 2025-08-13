{% docs int_sales_payment_method_monthly %}

Here is the explanation of the SQL code:

**Purpose:**
Calculate total revenue by product category for each month, handle edge cases where order_quantity is zero, and calculate the percentage of sales coming from each payment method.

**Logic Flow:**

* First, create three common table expressions (CTEs): PRODUCT_CTE, PRODUCT_CATEGORY_CTE, and TRANSFORMATION_CTE.
	+ PRODUCT_CTE joins the "stg_product" table to get product IDs and categories.
	+ PRODUCT_CATEGORY_CTE joins the "stg_product_category" table to get category names.
* In TRANSFORMATION_CTE, perform data transformations and aggregations:
	+ Calculate total revenue by product category for each month (REVENUE_MONTHLY).
	+ Calculate total revenue by product category and payment method for each month (REVENUE_MONTHLY_PM).
	+ Handle edge cases where order_quantity is zero.
	+ Calculate the percentage of sales coming from each payment method (REVENUE_METHOD_PCT_ONLY_ORDER_AMOUNT, REVENUE_PAYMENT_METHOD_PCT_EXCLUDING_SHIPPING, and REVENUE_PAYMENT_METHOD_PCT_INCLUDING_SHIPPING).
{% enddocs %}

