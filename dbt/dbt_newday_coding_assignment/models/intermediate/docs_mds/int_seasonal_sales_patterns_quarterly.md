{% docs int_seasonal_sales_patterns_quarterly %}

Here is the explanation of the SQL code:

**Purpose:**
Analyze seasonal sales patterns by displaying monthly sales trends, quarter-over-quarter growth rates, and identifying best and worst performing months for each product category.

**Logic Flow:**

* The SQL code starts by joining three tables (stg_sales_fact, stg_product, and stg_product_category) to obtain sales data grouped by order date, product category, quarter, and year.
* It then calculates the total revenue for each product category per quarter, along with the standard deviation of revenue (to measure volatility).
* The code also calculates the previous quarter's revenue using a lag function.
* The main query selects the year, category name, quarter number, current revenue, previous revenue, and the quarter-over-quarter growth rate (if available). If it's the first quarter, the growth rate is labeled as "1st Quarter".
* Finally, the results are ordered by year, category name, and quarter number.
{% enddocs %}

