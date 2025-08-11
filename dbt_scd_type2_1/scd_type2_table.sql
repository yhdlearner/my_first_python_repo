-- macros/scd_type2_merge_macro.sql
-- This macro implements generic Slowly Changing Dimension Type 2 logic.
-- It takes source, key, and change columns as inputs,
-- and manages the control table watermark.

{% macro scd_type2_merge_macro(
    source_table_name,      -- The name of the raw source table (e.g., 'events')
    source_timestamp_column,-- The timestamp column in the source for incremental filtering (e.g., 'last_updated_at')
    key_columns,            -- A LIST of unique business key columns (e.g., ['customer_id', 'product_id'])
    change_columns,         -- A list of columns that trigger a new version if changed (e.g., ['customer_name', 'address', 'email'])
    valid_from_column='valid_from', -- Name of the valid_from column in the target dimension
    valid_to_column='valid_to',     -- Name of the valid_to column in the target dimension
    is_current_column='is_current', -- Name of the is_current column in the target dimension
    control_table_ref='control_table' -- Reference to the control table model
) %}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=key_columns, # Now a list of columns
    partition_by={
      "field": valid_from_column,
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=key_columns + [is_current_column], # Cluster by all key columns and is_current
    full_refresh = false,

    # Post-hook to update the control table with the latest processed timestamp for this source
    post_hook="""
        MERGE INTO {{ ref(control_table_ref) }} AS T
        USING (
            SELECT
                '{{ source_table_name }}' AS source_name,
                MAX({{ source_timestamp_column }}) AS max_timestamp
            FROM {{ this }}
            WHERE dbt_batch_id = '{{ run_started_at.strftime("%Y%m%d%H%M%S") }}'
        ) AS S
        ON T.source_name = S.source_name
        WHEN MATCHED THEN
            UPDATE SET last_extracted_timestamp = S.max_timestamp, last_updated_at = CURRENT_TIMESTAMP(), loaded_by = 'dbt_scd2_generic_run'
        WHEN NOT MATCHED THEN
            INSERT (source_name, last_extracted_timestamp, loaded_by, last_updated_at) VALUES (S.source_name, S.max_timestamp, 'dbt_scd2_generic_run', CURRENT_TIMESTAMP());
    """
) }}

WITH source_incremental_data AS (
    -- Select only incremental records from the source based on the control table's watermark
    SELECT
        s.*, -- Select all columns from the source
        s.{{ source_timestamp_column }} AS __dbt_incremental_timestamp -- Alias for consistent use
    FROM
        {{ source('raw', source_table_name) }} s
    JOIN
        {{ ref(control_table_ref) }} ct ON ct.source_name = '{{ source_table_name }}'
    WHERE
        s.{{ source_timestamp_column }} > ct.last_extracted_timestamp
),

-- Calculate a hash for change detection on relevant columns
source_data_with_hash AS (
    SELECT
        *,
        FARM_FINGERPRINT(CONCAT(
            {% for col in change_columns %}
            COALESCE(CAST({{ col }} AS STRING), '__NULL__') {{ '||' if not loop.last else '' }}
            {% endfor %}
        )) AS row_hash
    FROM source_incremental_data
),

-- Identify the latest version of each record within the current batch of source data
latest_source_records AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY {% for col in key_columns %}{{ col }}{{ ', ' if not loop.last }}{% endfor %} ORDER BY __dbt_incremental_timestamp DESC) as rn
    FROM source_data_with_hash
)

SELECT
    {% for col in key_columns %}
    src.{{ col }},
    {% endfor %}
    {% for col in change_columns %}
    src.{{ col }},
    {% endfor %}
    src.__dbt_incremental_timestamp AS event_timestamp, -- Original timestamp from source
    -- For new records or new versions of changed records:
    -- valid_from is the current timestamp of the dbt run
    -- valid_to is NULL (as it's the current active record)
    -- is_current is TRUE
    CURRENT_TIMESTAMP() AS {{ valid_from_column }},
    CAST(NULL AS TIMESTAMP) AS {{ valid_to_column }},
    TRUE AS {{ is_current_column }},
    src.row_hash, -- Store the hash for future change detection
    '{{ run_started_at.strftime("%Y%m%d%H%M%S") }}' AS dbt_batch_id -- Unique ID for this dbt run
FROM
    latest_source_records src
WHERE
    src.rn = 1

{% endmacro %}