{{ 
    config(
        unique_key='LGA_CODE_2016', 
        alias='b_census_g01'
    ) 
}}

select *
from {{ source('raw', 'raw_census_g01') }}
