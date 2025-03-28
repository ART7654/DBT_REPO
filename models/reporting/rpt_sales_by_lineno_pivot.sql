{{config(materialized='view', schema='reporting_dev')}}

{% set orderlinenumber=get_order_linenos() %}

select
orderid,

{% for linenumber in orderlinenumber %}

sum(case when lineno={{linenumber}} then linesalesamount end) as line{{linenumber}}_sales,

{% endfor %}

sum(linesalesamount) as total_sales
from {{ref('fct_orders')}}
group by orderid