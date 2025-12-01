{{ config(
    unique_key='host_id',
    alias='s_host'
) }}

with source as (
    select * from {{ ref('b_listings') }}
),

cleaned as (
    select
        host_id,
        case
            when host_name = 'NaN' or host_name is null then 'Unknown'
            else trim(host_name)
        end as host_name,
        case
            when host_since = 'NaN' or host_since is null then null
            else to_date(host_since, 'DD/MM/YYYY')
        end as host_since,
        case
            when host_is_superhost = 'NaN' or host_is_superhost is null then null
            else host_is_superhost
        end as host_is_superhost,
        case
            when host_neighbourhood = 'NaN' or host_neighbourhood is null
            then null
            else trim(host_neighbourhood)
        end as host_neighbourhood,
        to_date(scraped_date, 'YYYY-MM-DD') as scraped_date 
    from source
)

select * from cleaned
