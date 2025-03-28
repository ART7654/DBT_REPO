{{config(materialized='table', schema=env_var('DBT_TRANSFORMSCHEMA', 'transforming_dev'))}}

select
c.CUSTOMERID,
c.COMPANYNAME,
c.CONTACTNAME,
c.city,
c.country,
d.divisionname,
c.address,
c.fax,
c.phone,
c.postalcode,
IFF(c.stateprovince = '', 'NA', c.stateprovince) as statename
from
{{ref('stg_customers')}} as c left join
{{ref('lkp_division')}} as d
on c.divisionid=d.divisionid

