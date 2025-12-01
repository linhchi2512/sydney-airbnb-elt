{% snapshot property_snapshot %}
{{
 config(
   strategy='timestamp',
   unique_key='listing_id',
   updated_at='scraped_date',
   alias='property_snapshot'
 )
}}

select distinct
  listing_id,
  property_type,
  room_type,
  accommodates,
  listing_neighbourhood,
  scraped_date  
from {{ ref('s_property') }}

{% endsnapshot %}