{{ config(materialized='table') }}

WITH int_companies as (
    select * from {{ ref('int_companies') }}
)
select company_pk, name, address, postal_code, city, country from int_companies 