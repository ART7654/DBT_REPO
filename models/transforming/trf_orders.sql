{{ config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA', 'transforming_dev')) }}
 
select
 
o.orderid,
od.lineno,
o.customerid,
o.employeeid,
o.shipperid,
od.productid,
od.quantity,
od.unitprice,
od.discount,
od.orderdate,
to_decimal((od.UnitPrice * od.Quantity) * (1-od.Discount), 9 , 2) as linesalesamount,
to_decimal((p.UnitCost * od.Quantity), 9, 2 ) as costofgoodssold,
to_decimal((((od.UnitPrice * od.Quantity) * (1-od.Discount)) - (p.UnitCost * od.Quantity)), 9 , 2 ) as margin
 
from
 
{{ref('stg_orders')}} as o inner join
 
{{ref('stg_order_details')}} as od
 
on o.orderid = od.orderid
 
inner join {{ref("stg_products")}} as p
 
on p.productid = od.productid