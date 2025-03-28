import snowflake.snowpark.functions as f
import pandas as p
import holidays

def is_holiday(orderdate):
    french_holiday=holidays.france()
    is_holiday=(orderdate in french_holiday)
    return is_holiday


def avgordervalue (total_sales,total_orders):
    return total_sales/total_orders

def model(dbt,session):

    dbt.config(materialzed='table',schema='reporting_dev')

    fct_orders_df=dbt.ref('fct_orders')
    dim_customers_df=dbt.ref('dim_customers')

    fct_orders_aggregate=(
                            fct_orders_df
                            .group_by('customerid')
                            .agg(
                                  f.min(f.col('orderdate')).alias('first_order_date'),
                                  f.max(f.col('orderdate')).alias('recent_order_date'),
                                  f.countDistinct(f.col('orderid')).alias('total_orders'),
                                  f.countDistinct(f.col('productid')).alias('total_products'),
                                  f.sum(f.col('quantity')).alias('total_quantity'),
                                  f.sum(f.col('linesalesamount')).alias('total_sales'),
                                  f.avg(f.col('margin')).alias('average_kargin')                                
                                )
                            )

    fct_customer_df=(
                    dim_customers_df
                    .join(fct_orders_aggregate,dim_customers_df.customerid==fct_orders_aggregate.customerid,'left')
                    .select(
                            dim_customers_df.companyname,
                            dim_customers_df.contactname,
                            dim_customers_df.city,
                            dim_customers_df.country,
                            fct_orders_aggregate.first_order_date,
                            fct_orders_aggregate.recent_order_date,
                            fct_orders_aggregate.total_orders,
                            fct_orders_aggregate.total_products,
                            fct_orders_aggregate.total_quantity,
                            fct_orders_aggregate.total_sales,
                            fct_orders_aggregate.average_kargin
                    )
    )

    final_df=fct_customer_df.withColumn('avg_order_value',avgordervalue(fct_customer_df['total_sales'],fct_customer_df['total_sales']))

    final_df =  final_df.filter(F.col('FIRST_ORDER_DATE').isNotNull())
 
    final_df = final_df.to_pandas()
 
    final_df["IS_FIRST_ORDER_DATE_ON_HOLIDAY"] = final_df["FIRST_ORDER_DATE"].apply(is_holiday)
    
    return final_df
