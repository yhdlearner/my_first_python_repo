-- models/dimensions/dim_customers_scd2.sql
-- Calls the generic SCD Type 2 merge macro for the customers dimension.

{{ dbt_production_scd2_process.scd_type2_merge_macro(
    source_table_name='events',               -- The raw source table name (as defined in sources.yml)
    source_timestamp_column='last_updated_at',-- The timestamp column in 'events'
    key_columns=['customer_id'],              -- NOW A LIST: The unique business key for customers
    change_columns=[                          -- List of columns that trigger a new version
        'customer_name',
        'address',
        'city',
        'state',
        'zip_code',
        'email'
    ],
    valid_from_column='valid_from',           -- Optional: default is 'valid_from'
    valid_to_column='valid_to',               -- Optional: default is 'valid_to'
    is_current_column='is_current',           -- Optional: default is 'is_current'
    control_table_ref='control_table'         -- Optional: default is 'control_table'
) }}
