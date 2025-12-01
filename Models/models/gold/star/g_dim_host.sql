{{
    config(
        unique_key='host_id',
        alias='g_dim_host'
    )
}}

select distinct
    host_id,
    host_name,
    host_since,
    case
        when host_is_superhost = 't' 
        then 1 else 0 
    end as host_is_superhost,
    host_neighbourhood,
    dbt_valid_from::timestamp as valid_from,
    dbt_valid_to::timestamp as valid_to
from {{ ref('host_snapshot') }}