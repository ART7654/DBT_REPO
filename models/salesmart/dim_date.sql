{{config(materialized='table',schema='salesmart_dev')}}

{% set min_date = get_min_orderdate() %}

{% set max_date = get_max_orderdate() %}

{{ dbt_date.get_date_dimension(min_date, max_date) }}