{% docs int_daily_sales_macro_model %}

Here's the explanation:

**Purpose:**
Calculate daily revenue totals for a given date range, including handling cases where no data exists.

**Logic Flow:**

• The SQL code uses Common Table Expressions (CTEs) to create intermediate result sets.
• In the first CTE, DAILY_RECORDS_CTE, sales data is filtered by order date, using a specific date range as an example (2024-01-15 to 2024-01-16).
• The second CTE, DAILY_REVENUE_CTE, calculates daily revenue totals by summing up order amounts and considering discounts and shipping costs.
• The final SELECT statement retrieves the calculated daily revenue totals from the DAILY_REVENUE_CTE.
{% enddocs %}

