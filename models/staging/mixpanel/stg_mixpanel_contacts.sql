WITH source as (
  SELECT * FROM {{ source('mixpanel', 'contacts') }}
), renamed as (
    SELECT email,
    first_name, 
    last_name,
    abandon_cart_count,
    account_created_count,
    gender,
    registration_date::timestamp as registration_year,
    city,
    region,
    last_event,
    last_purchase::timestamp as last_purchase,
    last_search::timestamp as last_search,
    last_share::timestamp as last_share
    FROM source
)
select * from renamed