{% docs int_customer_segmentation %}

Here is the explanation of the SQL code:

Purpose:
This dbt model segments customers into tiers based on their total purchase amount and includes customer names, along with the number of orders per customer.

Logic Flow:

• The first step is to create a CTE (Common Table Expression) called CTE_CUSTOMER that selects customer IDs and names from a staging table where the status is 'active'.
• The second step is to create another CTE called SALES_PER_CUSTOMER that calculates total orders and total purchase amount for each customer from a sales fact table.
• In the third step, the MERGE_CUST_SALES CTE is created by joining the two previous CTEs using a RIGHT JOIN, which includes customers who may not have placed any orders. The join is based on the customer ID.
• The final step is to apply a case statement to determine the customer tier (High Value, Medium Value, or Low Value) based on their total purchase amount.
{% enddocs %}

