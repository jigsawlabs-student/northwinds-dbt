{% set sources = ['stg_hubspot_contacts', 'stg_rds_customers'] %}

 with
  merged_contacts as (
    {% for source in sources %}
    SELECT 
    {{ 'contact_id' if 'hubspot' in source else 'null' }} as hubspot_contact_id,
      {{ 'customer_id' if 'rds' in source else 'null' }} as rds_contact_id,
    first_name, 
    last_name,
    phone,
     {{ 'company_id' if 'hubspot' in source else 'null' }} as hubspot_company_id,
        {{ 'company_id' if 'rds' in source else 'null' }} as rds_company_id
     FROM {{ ref(source)}}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
 ), final as (
     select 
     max(hubspot_contact_id) as hubspot_contact_id, max(rds_contact_id) as rds_contact_id,
        first_name, last_name, max(phone) as phone, 
        max(hubspot_company_id) as hubspot_company_id, max(rds_company_id) rds_company_id
     from merged_contacts
      group by first_name, last_name
 )
 select {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone']) }} as contact_pk,
  hubspot_contact_id, rds_contact_id,
  first_name, last_name, phone, hubspot_company_id, rds_company_id from final 