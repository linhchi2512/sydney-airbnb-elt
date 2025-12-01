{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

with base as (
    select
        distinct to_char(date_trunc('month', f.scraped_date), 'MM/YYYY') as month_year,
        s.lga_name as host_neighbourhood_lga,
        count(distinct f.host_id) as distinct_hosts,
        round(avg(f.price * (30 - f.availability_30)), 2) as avg_estimated_revenue,
        round(sum(f.price * (30 - f.availability_30)), 2) as total_estimated_revenue
    from {{ ref('g_fact') }} f
    left join {{ ref('g_dim_host') }} h 
        on f.host_id = h.host_id
        and f.scraped_date >= h.valid_from
        and (h.valid_to is null or f.scraped_date < h.valid_to)
    left join {{ ref('s_lga_suburb') }} s 
        on h.host_neighbourhood = s.suburb_name
    group by 1, 2
)

select
    month_year,
    host_neighbourhood_lga,
    distinct_hosts,
    avg_estimated_revenue,
    round(total_estimated_revenue / nullif(distinct_hosts, 0), 2) as estimated_revenue_per_host
from base
order by host_neighbourhood_lga, month_year