-- models/control/control_table.sql
-- This model manages the high-watermark for incremental loads for various sources.
-- It should be materialized as a table and updated via a post-hook
-- from the main dimension models using the generic SCD Type 2 macro.

{{ config(
    materialized='table',
    schema='control', -- Or your preferred metadata schema
    full_refresh = false -- Prevent accidental full refresh
) }}

SELECT
    CAST('initial_source' AS STRING) AS source_name, -- Identifier for the source table
    CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS last_extracted_timestamp,
    'initial_load' AS loaded_by,
    CURRENT_TIMESTAMP() AS last_updated_at
WHERE
    FALSE -- Ensures no rows are inserted on initial run, table is created empty
