{{
    config(
        unique_key='suburb_id',
        alias='s_lga_suburb'
    )
}}

select
    suburb_id::varchar as suburb_id,
    initcap(lower(lga_name)) as lga_name, 
    initcap(lower(suburb_name)) as suburb_name
from {{ ref("b_lga_suburb") }}

