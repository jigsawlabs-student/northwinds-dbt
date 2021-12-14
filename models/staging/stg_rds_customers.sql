WITH source as (
  SELECT * FROM {{ source('rds', 'customers')}} 
), 
renamed as (
    SELECT customer_id, country, 
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name
    FROM source
)
select * FROM source LIMIT 5000

