{{config(materialized = 'table')}}
 
select *
from
{{source('qwt_dev', 'raw_empoff')}}