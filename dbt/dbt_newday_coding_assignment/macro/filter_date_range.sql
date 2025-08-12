/*

## Question 7: 
Create a dbt macro that accepts date range parameters and filters sales data accordingly. 
Use this macro in a model to calculate daily revenue totals. Handle cases where no data exists for the given date range.



*/


{% macro filter_date_range(start_date, end_date, table_name, filter_column) %}

    {{ log("Running Job for the Range " ~ start_date ~ " to " ~ end_date ~ " on the table " ~ table_name, info=true) }}
    {# Build count query to check if data exists in range #}
    {% set sql_cnt =
        "select count(*) from " ~ table_name
        ~ " where " ~ filter_column
        ~ " between cast('" ~ start_date ~ "' as date)"
        ~ " and cast('" ~ end_date ~ "' as date)"
    %}

    {# Run the count query #}
    {% set results = run_query(sql_cnt) %}

    {% if execute %}
        {% set row_count = results.columns[0].values()[0] %}
    {% else %}
        {# During parse phase, assume zero rows #}
        {% set row_count = 0 %}
    {% endif %}

    {% if row_count > 0 %}
        {{ log("Data present for given date range", info=true) }}

        {{ table_name }}
        where {{ filter_column }} between cast('{{ start_date }}' as date)
        and cast('{{ end_date }}' as date)

    {% else %}
        {{ log("No Data for the given date range", info=true) }}

        {{ table_name }}
        where 1=2
    {% endif %}

{% endmacro %}