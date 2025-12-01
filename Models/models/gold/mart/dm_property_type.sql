{{
    config(
    alias='dm_property_type'
 )
}}

with base as (
    select
        distinct to_char(date_trunc('month', f.scraped_date), 'MM/YYYY') as month_year,
        p.property_type,
        p.room_type,
        p.accommodates,
        listing_neighbourhood,
        count(case when f.has_availability = 1 then 1 end) as active_listings,
        count(case when f.has_availability = 0 then 1 end) as inactive_listings,
        round(sum(case when f.has_availability = 1 then 1 end)::numeric / count(*)::numeric * 100, 2) as active_listing_rate,
        round(min(case when f.has_availability = 1 then f.price else null end), 2) as min_active_price,
        round(max(case when f.has_availability = 1 then f.price else null end), 2) as max_active_price,
        percentile_cont(0.5) within GROUP (order by f.price) filter (where f.has_availability = 1
        ) as median_active_price,
        round(avg(case when f.has_availability = 1 then f.price else null end), 2) as avg_active_price,
        count(distinct f.host_id) as distinct_hosts,
        round(count(distinct case when h.host_is_superhost = 1 then 1 end)::numeric / nullif(count(distinct f.host_id), 0)::numeric * 100, 2) as superhost_rate,
        round(avg(case when f.has_availability = 1 then f.review_scores_rating else null end), 2) as avg_active_review_score,
        sum(30 - f.availability_30) as total_stays,
        round(avg(case when f.has_availability = 1 then (30 - f.availability_30) * f.price end), 2) as avg_estimated_revenue_per_active_listing     
    from {{ ref('g_fact') }} f
    left join {{ ref('g_dim_property')}} p
        on f.listing_id = p.listing_id
        and f.scraped_date >= p.valid_from 
        and (p.valid_to is null or f.scraped_date < p.valid_to)
    left join {{ ref('g_dim_host') }} h 
        on f.host_id = h.host_id 
        and f.scraped_date >= h.valid_from 
        and (h.valid_to is null or f.scraped_date < h.valid_to)
    group by 1, 2, 3, 4, 5
),

changes as (
    select
        property_type,
        room_type,
        accommodates,
        month_year,
        listing_neighbourhood,
        active_listing_rate,
        min_active_price,
        max_active_price,
        median_active_price,
        avg_active_price,
        distinct_hosts,
        superhost_rate,
        avg_active_review_score,
        round(
            (active_listings - lag(active_listings) over (partition by property_type, room_type, accommodates order by month_year))
            / nullif(lag(active_listings) over (partition by property_type, room_type, accommodates order by month_year), 0)::numeric * 100,
            2
        ) as percentage_change_active_listings,
        round(
            (inactive_listings - lag(inactive_listings) over (partition by property_type, room_type, accommodates order by month_year))
            / nullif(lag(inactive_listings) over (partition by property_type, room_type, accommodates order by month_year), 0)::numeric * 100,
            2
        ) as percentage_change_inactive_listings,
        total_stays,
        avg_estimated_revenue_per_active_listing
    from base
)

select
    property_type,
    room_type,
    accommodates,
    month_year,
    listing_neighbourhood,
    active_listing_rate,
    min_active_price,
    max_active_price,
    median_active_price,
    avg_active_price,
    distinct_hosts,
    superhost_rate,
    avg_active_review_score,
    percentage_change_active_listings,
    percentage_change_inactive_listings,
    total_stays,
    avg_estimated_revenue_per_active_listing
from changes
order by 1, 2, 3, 4