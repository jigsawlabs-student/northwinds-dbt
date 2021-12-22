with contacts as (
   select * from {{ ref('int_contacts') }}
),
companies as (
   select * from {{ ref('int_companies') }}
),
 contacts_dim as (
  SELECT
    {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone']) }} as contact_pk,
    *
  FROM contacts
), final as (
    select first_name, last_name, phone, company_pk from contacts_dim join companies on 
  companies.rds_company_id = contacts_dim.rds_company_id or companies.hubspot_company_id = contacts_dim.hubspot_company_id
)
select * from final