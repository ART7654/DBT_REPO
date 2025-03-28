{% snapshot shipment_snapshot %}

{{
    config 
    (
        target_database='QWT_ANALYTICS',
        target_schema='SNAPSHOT_DEV',
        unique_key=  "orderid || '- ' || lineno",
        
        strategy='timestamp',
        updated_at='shipmentdate'
    )
}}

select * from {{ref('stg_shipment')}}

{% endsnapshot %}