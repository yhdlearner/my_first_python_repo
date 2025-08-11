-- models/scd2_orders_transformed.sql

{{ config(
  materialized='incremental',
  partition_by={'field': 'effective_from', 'data_type': 'timestamp'},
  clustering=['order_id']
) }}

{% set transformed_source %}
select
  order_id,
  customer_id,
  order_status,
  order_total,
  start_date                       as source_start_ts
from {{ ref('raw_orders') }}
where order_status != 'TEST'
{% endset %}

{{ scd2_loader(
    source_query      = transformed_source,
    target_relation   = this,
    key_columns       = ['order_id'],
    change_columns    = ['customer_id','order_status','order_total'],
    start_date_column = 'source_start_ts'
) }}
