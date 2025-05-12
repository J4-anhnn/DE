{% snapshot orders_snapshot %}

{{
    config(
      target_database='mydatabase',
      target_schema='snapshots',
      unique_key='order_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('raw_data', 'orders') }}

{% endsnapshot %}
