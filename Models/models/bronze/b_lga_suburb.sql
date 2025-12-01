{{
    config(
        unique_key='suburb_id',
        alias='b_lga_suburb'
    )
}}

select
{{ dbt_utils.generate_surrogate_key(['LGA_NAME', 'SUBURB_NAME']) }} as suburb_id
, *
from {{ source('raw', 'raw_suburb') }}
