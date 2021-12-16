WITH source as (
  SELECT * FROM {{ source('rds', 'customers')}} 
), 
renamed as (
    SELECT 
    customer_id, 
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name,
    phone,
    company_name,
    country
    FROM source
),transformed as (
    SELECT
    customer_id,
    first_name, 
    last_name, 
    REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') as phone,
    company_name,
    country
    FROM renamed
), final as (
    SELECT 
    customer_id,
    first_name,
    last_name,
      CASE WHEN LENGTH(phone) = 10 THEN
       '(' || SUBSTRING(phone, 1, 3) || ') ' || 
       SUBSTRING(phone, 4, 3) || '-' ||
       SUBSTRING(phone, 7, 4) 
       END as phone,
       company_name,
       country
      FROM transformed
)
SELECT * FROM final


