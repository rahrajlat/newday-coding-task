with src as (
    select * from {{ source('raw', 'customers') }}
)
select
    cast(customer_id as integer) as customer_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    lower(email) as email,
    cast(created_at as date) as created_at,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk
from src
