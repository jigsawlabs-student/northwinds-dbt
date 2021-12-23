with hubspot_companies as (
    select * from {{ ref('stg_hubspot_companies') }}
), rds_companies as (
    select * from {{ ref('stg_rds_companies') }}
), merged_companies as (
    select company_id as hubspot_company_id, null as rds_company_id, name from hubspot_companies union all
    select null as hubspot_company_id, company_id as rds_company_id, name from rds_companies
), deduped as (
    select 
    max(hubspot_company_id) as hubspot_company_id,
    max(rds_company_id) as rds_company_id, name
     from merged_companies group by name
)
 select {{ dbt_utils.surrogate_key(['deduped.name']) }} as company_pk, hubspot_company_id, rds_company_id, deduped.name, address,
   postal_code, city, country from deduped
  join rds_companies on rds_companies.company_id = deduped.rds_company_id

