{{
 config(
 unique_key='lga_code_2016',
 alias='s_census_g02'
 )
}}

select
    RIGHT(lga_code_2016, 5)         as lga_code_2016,
    Median_age_persons              as median_age_persons,
    Median_mortgage_repay_monthly   as median_mortgage_repay_monthly,
    Median_tot_prsnl_inc_weekly     as median_total_personal_income_weekly,
    Median_rent_weekly              as median_rent_weekly,
    Median_tot_fam_inc_weekly       as median_total_family_income_weekly,
    Average_num_psns_per_bedroom    as average_number_persons_per_bedroom,
    Median_tot_hhd_inc_weekly       as median_total_household_income_weekly,
    Average_household_size          as average_household_size
from {{ ref('b_census_g02') }}