{{ 
    config(
        unique_key='id', 
        alias='s_listings'
    ) 
}}

with
    source as (select * from {{ ref('b_listings') }}),
    cleaned as (
        select
            id::varchar as id,
            listing_id,
            scrape_id,
            to_date(scraped_date, 'YYYY-MM-DD') as scraped_date,
            host_id,
            price::numeric as price,
            has_availability,
            availability_30::int as availability_30,
            number_of_reviews::int as number_of_reviews,
            case
                when review_scores_rating = 'NaN' or review_scores_rating is null
                then null
                else review_scores_rating::numeric
            end as review_scores_rating,

            case
                when review_scores_accuracy = 'NaN' or review_scores_accuracy is null
                then null
                else review_scores_accuracy::numeric
            end as review_scores_accuracy,

            case
                when review_scores_cleanliness = 'NaN' or review_scores_cleanliness is null
                then null
                else review_scores_cleanliness::numeric
            end as review_scores_cleanliness,

            case
                when review_scores_checkin = 'NaN' or review_scores_checkin is null
                then null
                else review_scores_checkin::numeric
            end as review_scores_checkin,

            case
                when review_scores_communication = 'NaN' or review_scores_communication is null
                then null
                else review_scores_communication::numeric
            end as review_scores_communication,

            case
                when review_scores_value = 'NaN' or review_scores_value is null
                then null
                else review_scores_value::numeric
            end as review_scores_value
        from source
    )
select * from cleaned
