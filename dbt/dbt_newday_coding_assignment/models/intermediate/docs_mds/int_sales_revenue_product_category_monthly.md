{% docs int_sales_revenue_product_category_monthly %}

Here is the explanation of the SQL code's purpose and logic:

**Purpose:**
Calculate total revenue by product category for each month, considering three scenarios: pure Order Amount, Order Amount with discounts applied, and Order Amount with both discounts and shipping costs applied.

**Logic Flow:**

• The model starts by creating two Common Table Expressions (CTEs): PRODUCT_CTE to bring in the product_category_id, and PRODUCT_CATEGORY_CTE to bring in the category_name.
• It then joins these CTEs with the stg_sales_fact table, which contains order data, to create a transformation CTE (TRANSFORMATION_CTE) that applies aggregations and transformations to calculate total revenue for each product category and month.
• The transformations include calculating net revenue excluding shipping costs (Net Revenue Excl. Shipping) and including shipping costs (Net Revenue Incl. Shipping), as well as pure Order Amount.
• Finally, the model selects the desired columns from the TRANSFORMATION_CTE to produce the final output.
{% enddocs %}

