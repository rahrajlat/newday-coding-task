{% docs stg_weather_data %}

Here is the explanation:

**Purpose:**
The SQL code is retrieving weather data from a raw database table and storing it in a temporary view.

**Logic Flow:**

• The SQL code starts by defining a Common Table Expression (CTE) named CTE_WEATHER_DATA.
• Inside the CTE, it selects all columns (*) from the weather_data table in the raw database using Jinja templating to specify the source.
• The CTE is then used as a temporary result set for the main query.
• Finally, the SQL code selects all columns (*) from the CTE_WEATHER_DATA CTE and returns the results.
{% enddocs %}

