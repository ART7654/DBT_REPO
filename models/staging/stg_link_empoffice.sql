{{config(materialized = 'table')}}
 
select
 
md5(ho.officehashkey || re.empid) as empofficehashkey,
current_timestamp as loaddate,
ho.recordsource,
ho.officehashkey,
re.empid
 
from
{{ref('stg_hub_empoffice')}} as ho
inner join
{{source('qwt_dev', 'raw_employees' )}} as re
on re.office = ho.office