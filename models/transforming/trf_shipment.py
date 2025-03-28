import snowflake.snowpark.functions as f

def model(dbt,session):

    shipment_df=dbt.ref('shipment_snapshot')
    shipper_df=dbt.ref('lkp_shippers')

    shipment_transofrmed_df=(
                                shipment_df
                                .join(shipper_df, shipment_df.shipperid==shipper_df.shipperid,'left')
                                .select(
                                        shipment_df.orderid,
                                        shipment_df.lineno,
                                        shipper_df.companyname,
                                        shipment_df.shipmentdate,
                                        shipment_df.status.alias('currentstatus')
                                        )
                            )

    shipment_currentstatus_df=shipment_transofrmed_df.filter(f.col('DBT_VALID_TO').isNull())

    return shipment_currentstatus_df