{{config(materialized='table',schema=env_var('DBT_TRANSFORMSCHEMA', 'transforming_dev'))}}


select
p.productid,
p.productname,
s.companyname as suppliercompany,
s.contactname as suppliercontact,
s.city as suppliercity,
s.country as suppliercountry,
c.categoryname,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
iff(p.unitsonorder>p.unitsinstock,'Not Available','Available') as StockAvailibility

from
{{ref('stg_products')}} p join
{{ref("trf_suppliers")}} s
on p.SupplierID=s.SupplierID
join {{ref("lkp_categories")}} c
on c.CategoryID =p.CategoryID
