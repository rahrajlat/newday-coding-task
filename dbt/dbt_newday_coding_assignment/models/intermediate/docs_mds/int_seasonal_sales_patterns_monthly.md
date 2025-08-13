{% docs int_seasonal_sales_patterns_monthly %}

Here is the explanation of the SQL code:

**Purpose:**
The purpose of this SQL code is to analyze seasonal sales patterns by providing monthly sales trends, quarter-over-quarter growth rates, and identifying the best and worst performing months for each product category. Additionally, it calculates the coefficient of variation to measure sales volatility.

**Logic Flow:**

* First, the code joins multiple tables (stg_sales_fact, stg_product, and stg_product_category) based on common columns (product_id).
* It then creates a temporary table (SALES_PER_MONTH) that summarizes sales data by product category and month.
* The code calculates revenue for each month by taking into account discounts and shipping costs.
* It also assigns a ranking to the best-performing month for each category using the ROW_NUMBER function.
* In the final SELECT statement, the code provides additional information such as the best/worst performing months, quarter-over-quarter growth rates, and the coefficient of variation (CO-efficient) to measure sales volatility.
{% enddocs %}

