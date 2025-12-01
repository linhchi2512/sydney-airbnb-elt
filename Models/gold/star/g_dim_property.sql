{{
    config(
        unique_key='property_id',
        alias='g_dim_property'
    )  
}}

with 
source as (
    select * from {{ ref('property_snapshot') }}
),
cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['listing_id', 'dbt_valid_from']) }} as property_id,
        listing_id,
        property_type,
        room_type,
        accommodates,
        listing_neighbourhood,
        dbt_valid_from::timestamp as valid_from,
        dbt_valid_to::timestamp as valid_to
    from source
)
select * from cleaned