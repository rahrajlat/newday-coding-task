{% docs int_flag_customer_orders %}

Here is the explanation of the SQL code:

**Purpose:**
The SQL code creates a dbt model that flags orders for review based on business rules, including discount applied and shipping cost exceeding certain percentages.

**Logic Flow:**

* The first common table expression (CTE), SALES_FACT_CTE, selects relevant columns from a sales fact table, handles null values in discount_applied and shipping_cost, and calculates a flag percentage.
* The second CTE, TRANSFORMATION_CTE, applies the business rules to the data by checking if the discount_applied exceeds 30% or the shipping_cost exceeds the calculated flag percentage. If either condition is true, it sets the FLAGGED_TRANSACTION column to 'Y'.
* The code also generates a string of reasons for the flagged transactions using concatenation and conditional statements.
* Finally, the model selects all columns from the TRANSFORMATION_CTE CTE to produce the final output.
{% enddocs %}

