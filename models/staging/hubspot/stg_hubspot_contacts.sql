WITH contacts as (
  SELECT * FROM {{ source('hubspot', 'contacts') }}
), 
companies as (
  SELECT * FROM {{ ref('stg_hubspot_companies') }}
), 
renamed as (
    SELECT 
    concat('hubspot-', hubspot_id) as contact_id,
    first_name,
    last_name,
     REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') as updated_phone,
     CASE WHEN LENGTH(updated_phone) = 10 THEN
       '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
       SUBSTRING(updated_phone, 4, 3) || '-' ||
       SUBSTRING(updated_phone, 7, 4) 
       END as phone,
    companies.company_id
    FROM contacts 
    JOIN companies ON contacts.business_name = companies.name
), final as (
    SELECT
    contact_id,
    first_name, 
    last_name,
    phone,
    company_id
    FROM renamed
)
SELECT * FROM final