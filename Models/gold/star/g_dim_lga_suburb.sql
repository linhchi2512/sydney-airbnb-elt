{{ 
    config(
        unique_key="suburb_id", 
        alias="g_dim_lga_suburb"
    ) 
}}

select 
    s.suburb_id,
    c.lga_code,
    s.lga_name,
    s.suburb_name
from 
    {{ ref("s_lga_suburb") }} as s
inner join 
    {{ ref("s_lga_code") }} as c
on 
    s.lga_name = c.lga_name 