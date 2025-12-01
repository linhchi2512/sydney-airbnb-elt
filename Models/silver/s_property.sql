{{ config(
    unique_key='listing_id',
    alias='s_property'
) }}

with source as (
    select * from {{ ref('b_listings') }}
),

cleaned as (
    select 
        listing_id,
        to_date(scraped_date, 'YYYY-MM-DD') as scraped_date,
        trim(listing_neighbourhood) as listing_neighbourhood,
        trim(property_type) as property_type,
        trim(room_type) as room_type,
        accommodates,
        host_id
    from source
)

select * from cleaned
