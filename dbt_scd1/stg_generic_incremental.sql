-- models/staging/stg_generic_incremental.sql
-- This is a generic staging model that selects incremental records from a source
-- based on a watermark from the control table.
-- It's intended to be called by other models/macros.

-- This model is typically materialized as a view or ephemeral.
-- The actual configuration will be set by the calling model or in dbt_project.yml.

-- This model is not directly runnable, but acts as a template.
-- It will be referenced by the generic SCD Type 2 macro.
-- The `source_name` and `timestamp_column` will be passed dynamically.

-- Example of how the macro will construct the query:
-- SELECT
--     s.*,
--     s.{{ timestamp_column }} AS __dbt_incremental_timestamp
-- FROM
--     {{ source('raw', source_table_name) }} s
-- JOIN
--     {{ ref('control_table') }} ct ON ct.source_name = '{{ source_table_name }}'
-- WHERE
--     s.{{ timestamp_column }} > ct.last_extracted_timestamp