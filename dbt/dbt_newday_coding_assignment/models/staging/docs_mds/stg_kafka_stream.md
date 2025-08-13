{% docs stg_kafka_stream %}

Here is the explanation of the SQL code:

Purpose:
This SQL code retrieves data from a Kafka stream and loads it into a temporary view, which can then be queried or further processed.

Logic Flow:
• The query starts by creating a temporary view called CTE_KAFKA_STREAM using a Common Table Expression (CTE).
• Inside the CTE, the query selects all columns (*) from a Kafka stream source, specified by the {{ source('raw', 'kafka_stream') }} syntax.
• The resulting dataset is stored in the temporary view CTE_KAFKA_STREAM.
• Finally, the query selects all columns (*) from the temporary view CTE_KAFKA_STREAM, effectively retrieving the data loaded into it.
{% enddocs %}

