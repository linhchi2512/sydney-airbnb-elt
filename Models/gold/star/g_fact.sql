{{
    config(
        unique_key='listing_id',
        alias='g_fact'
    )
}}

select
    listing_id,
    host_id,
    scraped_date,
    price,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value,
    case
        when has_availability = 't' 
        then 1 else 0 
    end as has_availability
from {{ ref('s_listings') }}