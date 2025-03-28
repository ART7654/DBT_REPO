import snowflake.snowpark.functions as f

def model(dbt,session):
    dbt.config(materialized='table')
    stg_customers_df=dbt.source('qwt_dev','raw_customer')

    return stg_customers_df