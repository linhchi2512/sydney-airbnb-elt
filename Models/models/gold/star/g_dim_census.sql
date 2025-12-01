{{ 
    config(
        unique_key="lga_code_2016", 
        alias="g_dim_census"
    ) 
}}

select *
from {{ ref("s_census_g01") }} as g01
left join {{ ref("s_census_g02") }} as g02 
using(lga_code_2016)