WITH source as (
  SELECT * FROM {{ source('rds', 'customers')}} 
), 
renamed as (
    SELECT 
    concat('rds-', customer_id) as contact_id,
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name,
    REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') as phone,
    company_name as business_name,
    contact_title
    FROM source
), final as (
    SELECT
    contact_id as customer_id,
    first_name, 
    last_name,
    CASE WHEN LENGTH(phone) = 10 THEN
      '(' || SUBSTRING(phone, 1, 3) || ') ' || 
      SUBSTRING(phone, 4, 3) || '-' ||
      SUBSTRING(phone, 7, 4) 
      END as phone,
      business_name,
      contact_title
      FROM renamed
)
select * FROM final

