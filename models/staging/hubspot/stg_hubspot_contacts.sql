WITH source as (
  SELECT * FROM {{ source('hubspot', 'contacts') }}
), renamed as (
    SELECT 
    hubspot_id as customer_id,
    first_name,
     last_name, 
     REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') as phone,
      business_name as company_name
      FROM source
), final as (
    SELECT 
    hubspot_id,
    first_name,
     last_name,
      CASE WHEN LENGTH(phone) = 10 THEN
       '(' || SUBSTRING(phone, 1, 3) || ') ' || 
       SUBSTRING(phone, 4, 3) || '-' ||
       SUBSTRING(phone, 7, 4) 
       END as phone,
       company_name 
      FROM renamed
)
SELECT * FROM final