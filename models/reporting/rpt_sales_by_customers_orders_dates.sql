{{config(materialized='view',schema='reporting_dev')}}

select
c.COMPANYNAME,
c.CONTACTNAME,
max(o.ORDERDATE) as first_order_date,
min(o.ORDERDATE) as recent_order_date,
count(distinct o.orderid) as total_orders,
count(distinct o.productid) as total_products,
sum(o.quantity) as total_quantity,
sum(o.linesalesamount) as total_sales,
avg(MARGIN) as avg_margin
from {{ref('dim_customers')}} c
join {{ref('fct_orders')}} o
on c.CUSTOMERID=o.CUSTOMERID
group by c.COMPANYNAME,c.CONTACTNAME