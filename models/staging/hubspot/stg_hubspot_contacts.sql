WITH source as (
  SELECT * FROM {{ source('hubspot', 'contacts') }}
), renamed as (
    SELECT first_name, last_name, REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') as phone, business_name FROM source
), final as (
    SELECT first_name, last_name,
      CASE WHEN LENGTH(phone) = 10 THEN
       '(' || SUBSTRING(phone, 1, 3) || ') ' || 
       SUBSTRING(phone, 4, 3) || '-' ||
       SUBSTRING(phone, 7, 4) 
       END as phone,
       business_name
      FROM renamed
)
SELECT * FROM final