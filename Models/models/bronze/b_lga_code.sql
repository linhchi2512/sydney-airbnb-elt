{{
    config(
        unique_key='LGA_CODE',
        alias='b_lga_code'
    )
}}

select * from {{ source('raw', 'raw_code') }}