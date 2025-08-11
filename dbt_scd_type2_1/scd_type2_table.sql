{{ config(
    materialized = 'incremental',
    partition_by = {'field': 'effective_from', 'data_type': 'timestamp'},
    clustering   = ['order_id']
) }}

{{ scd2_loader(
    source_relation = ref('raw_orders'),
    target_relation = this,
    key_columns      = ['order_id'],
    change_columns   = ['customer_id', 'order_status', 'order_total'],
    start_date_column= 'start_date'
) }}
