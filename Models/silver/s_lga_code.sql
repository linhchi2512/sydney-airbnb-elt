{{
 config(
 unique_key='lga_code',
 alias='s_lga_code'
 )
}}

select
    lga_code::varchar as lga_code,
    trim(lga_name) as lga_name
from {{ ref('b_lga_code') }}
