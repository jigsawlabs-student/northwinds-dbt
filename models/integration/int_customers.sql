--  with customers_merged as (
--     SELECT first_name, last_name, phone, business_name FROM   {{ ref('stg_hubspot_contacts') }}
--     union all 
--     SELECT first_name, last_name, phone, business_name FROM   {{ ref('stg_rds_customers') }}
--  )
select * from {{ ref('stg_hubspot_contacts') }}




 