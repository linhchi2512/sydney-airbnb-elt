{% snapshot host_snapshot %}
{{
 config(
   strategy='timestamp',
   unique_key='host_id',
   updated_at='scraped_date',
   alias='host_snapshot'
 )
}}

select distinct
  host_id,
  host_name,
  host_since,
  host_is_superhost,
  host_neighbourhood,
  scraped_date
from {{ ref('s_host') }}

{% endsnapshot %}