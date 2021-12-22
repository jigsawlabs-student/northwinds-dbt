 with contacts as (
     select * from {{ ref('stg_hubspot_contacts') }}
 ), customers as (
     select * from {{ ref('stg_rds_customers') }} 
 ),
  merged_contacts as (
    SELECT 
    contact_id as hubspot_contact_id,
    null as rds_contact_id,
    first_name, 
    last_name,
    phone, 
    company_id as hubspot_company_id,
    null as rds_company_id
     FROM contacts
    union all 
    SELECT 
    null as hubspot_contact_id,
    customer_id as rds_contact_id,
    first_name, 
    last_name,
    phone, 
    null as hubspot_company_id,
    company_id as rds_company_id
    FROM customers
 ), final as (
     select 
     max(hubspot_contact_id) as hubspot_contact_id, max(rds_contact_id) as rds_contact_id,
        first_name, last_name, max(phone) as phone, 
        max(hubspot_company_id) as hubspot_company_id, max(rds_company_id) rds_company_id
     from merged_contacts
      group by first_name, last_name
 )
 select {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone']) }} as contact_pk, hubspot_contact_id, rds_contact_id,
  first_name, last_name, phone, hubspot_company_id, rds_company_id from final 