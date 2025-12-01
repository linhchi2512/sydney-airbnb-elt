{{
    config(
        unique_key='lga_code',
        alias='g_dim_lga_code'
 )
}}

select *
from {{ ref('s_lga_code') }}