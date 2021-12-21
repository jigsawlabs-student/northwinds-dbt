 with contacts as (
     select * from {{ ref('stg_hubspot_contacts') }}
 ), customers as (
     select * from {{ ref('stg_rds_customers') }} 
 ),
  merged_contacts as (
    SELECT contact_id,
    first_name, 
    last_name,
    phone FROM contacts
    union all 
    SELECT customer_id as contact_id,
    first_name, 
    last_name,
    phone FROM customers
 ), final as (
     select array_agg(contact_id) as contact_ids, first_name, last_name, max(phone) as phone
     from merged_contacts group by first_name, last_name ORDER BY last_name
 )
 select * from final