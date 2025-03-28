{{config(materialized='table')}}

select
orderid ,
lineno  ,
shipperid   ,
customerid  ,
productid   ,
employeeid  ,
split_part(shipmentdate,' ',1)::date as shipmentdate,
status  
from 
{{ source('qwt_dev','raw_shipment') }}
