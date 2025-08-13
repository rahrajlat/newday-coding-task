{% docs int_payment_method_metrics %}

Here is the explanation of the SQL code in the fixed format:

Purpose:
This SQL code analyzes payment method preferences by calculating total revenue, average order value, number of orders, and percentage distribution for each payment method.

Logic Flow:
• The query starts by creating a Common Table Expression (CTE) named PAYMENT_REVENUE that calculates total revenue by payment method, excluding shipping costs.
• Another CTE named AVG_PAYMENT_ORDER_VALUE calculates the average order value by payment method.
• A third CTE named NO_OF_PAYMENT_ORDERS counts the number of orders for each payment method.
• The query then joins these three CTES based on the payment method and calculates additional metrics such as total revenue including shipping costs, and percentage distribution for each payment method.
{% enddocs %}

