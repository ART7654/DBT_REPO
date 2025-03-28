{{ config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'transforming_dev')) }}

select 
get(xmlget(customerinfo, 'SupplierID'),'$') as SupplierID,
get(xmlget(customerinfo, 'CompanyName'),'$') ::varchar as CompanyName,
get(xmlget(customerinfo, 'ContactName'),'$')::varchar as ContactName,
get(xmlget(customerinfo, 'Address'),'$')::varchar as Address,
get(xmlget(customerinfo, 'City'),'$')::varchar as City,
get(xmlget(customerinfo, 'PostalCode'),'$')::varchar as PostalCode,
get(xmlget(customerinfo, 'Country'),'$')::varchar as Country,
get(xmlget(customerinfo, 'Phone'),'$')::varchar as Phone,
get(xmlget(customerinfo, 'Fax'),'$')::varchar as Fax
from 
{{ref('stg_suppliers')}}