/*

This was created to reduce the code complexity

*/


{% macro get_month_name_from_date(date_col) %}
 {{ log("Running get_month_name_from_date for " ~ date_col, info=true) }}
    CASE 
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 1  THEN 'JAN'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 2  THEN 'FEB'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 3  THEN 'MAR'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 4  THEN 'APR'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 5  THEN 'MAY'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 6  THEN 'JUN'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 7  THEN 'JUL'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 8  THEN 'AUG'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 9  THEN 'SEP'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 10 THEN 'OCT'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 11 THEN 'NOV'
        WHEN EXTRACT(MONTH FROM {{ date_col }}) = 12 THEN 'DEC'
    END
{% endmacro %}
