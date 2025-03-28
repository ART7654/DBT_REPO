{{config (materialized='incremental', unique_key = ['orderid','lineno'])}}

select 
od.OrderID	,
od.LineNo	,
od.ProductID,
od.Quantity,	
od.UnitPrice,
od.Discount ,
o.orderdate
from 
{{ source('qwt_dev', 'raw_orders') }} o
inner join 
{{ source('qwt_dev', 'raw_order_details') }} od
on o.orderid=od.orderid


{% if is_incremental() %}

where  o.orderdate > (select max(orderdate) from {{this}})

{% endif %}