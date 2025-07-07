-- Stored Procedure: `scd_type2_merge`
-- Description: Implements Slowly Changing Dimension Type 2 (SCD Type 2) logic
--              between a source table and a target dimension table in BigQuery.
--              It identifies new records, updates changed records by expiring
--              the old version and inserting a new one, and handles null comparisons.
--
-- Parameters:
--   p_source_table_name (STRING): Full path to the source table (e.g., 'project.dataset.source_table').
--   p_target_table_name (STRING): Full path to the target dimension table (e.g., 'project.dataset.target_dim_table').
--   p_key_columns_csv (STRING): Comma-separated string of column names that form the unique key
--                               for a record in the source table (e.g., 'customer_id,product_id').
--   p_change_columns_csv (STRING): Comma-separated string of column names that, if changed,
--                                  trigger a new version in the target table
--                                  (e.g., 'address,phone_number,email').
--   p_start_date_column (STRING): Name of the column in the target table that stores the
--                                 start date/timestamp of the record's validity (e.g., 'valid_from').
--   p_end_date_column (STRING): Name of the column in the target table that stores the
--                               end date/timestamp of the record's validity (e.g., 'valid_to').
--   p_is_current_column (STRING): Name of the boolean column in the target table that indicates
--                                 if the record is currently active (e.g., 'is_current').
--   p_timestamp_column (STRING): Name of a timestamp column in the source table that can be used
--                                for ordering if multiple changes occur within the same execution window.
--                                (e.g., 'last_updated_at'). If not provided, CURRENT_DATETIME() is used.
--
-- Example Usage:
-- CALL `project.dataset.scd_type2_merge`(
--   'project.dataset.stg_customers',
--   'project.dataset.dim_customers',
--   'customer_id',
--   'customer_name,address,city,state,zip_code',
--   'valid_from',
--   'valid_to',
--   'is_current',
--   'last_updated_at'
-- );
--
CREATE OR REPLACE PROCEDURE `scd_type2_merge`(
    p_source_table_name STRING,
    p_target_table_name STRING,
    p_key_columns_csv STRING,
    p_change_columns_csv STRING,
    p_start_date_column STRING,
    p_end_date_column STRING,
    p_is_current_column STRING,
    p_timestamp_column STRING DEFAULT NULL
)
BEGIN
    -- Declare variables for dynamic SQL construction
    DECLARE v_key_columns_array ARRAY<STRING>;
    DECLARE v_change_columns_array ARRAY<STRING>;
    DECLARE v_key_join_condition STRING;
    DECLARE v_change_detection_condition STRING;
    DECLARE v_all_source_columns STRING;
    DECLARE v_merge_sql STRING;
    DECLARE v_temp_table_name STRING;
    DECLARE v_current_timestamp DATETIME;

    -- Set the current timestamp for validity dates
    SET v_current_timestamp = CURRENT_DATETIME();

    -- Parse key and change columns from CSV strings into arrays
    SET v_key_columns_array = SPLIT(p_key_columns_csv, ',');
    SET v_change_columns_array = SPLIT(p_change_columns_csv, ',');

    -- Construct the JOIN condition for key columns
    -- Example: 'T.customer_id = S.customer_id AND T.product_id = S.product_id'
    SET v_key_join_condition = (
        SELECT STRING_AGG(
            FORMAT('T.%s = S.%s', TRIM(col), TRIM(col)), ' AND '
        )
        FROM UNNEST(v_key_columns_array) AS col
    );

    -- Construct the CHANGE DETECTION condition for change columns, handling NULLs
    -- Example: '(T.address IS NULL AND S.address IS NULL) OR (T.address = S.address) OR (T.address IS NOT NULL AND S.address IS NOT NULL AND T.address != S.address)'
    -- More robust NULL handling: (T.col IS DISTINCT FROM S.col) or (T.col != S.col OR (T.col IS NULL != S.col IS NULL))
    -- The below logic explicitly checks for equality or both being NULL, and inequality if both are non-NULL.
    SET v_change_detection_condition = (
        SELECT STRING_AGG(
            FORMAT('((T.%s IS NULL AND S.%s IS NULL) OR (T.%s IS NOT NULL AND S.%s IS NOT NULL AND T.%s != S.%s))',
                   TRIM(col), TRIM(col), TRIM(col), TRIM(col), TRIM(col), TRIM(col)), ' OR '
        )
        FROM UNNEST(v_change_columns_array) AS col
    );

    -- Get all column names from the source table to use in the INSERT statement
    -- This ensures all columns from the source are carried over to the new target record
    SET v_all_source_columns = (
        SELECT STRING_AGG(column_name, ', ')
        FROM `region-us`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = SPLIT(p_source_table_name, '.') [OFFSET(2)]
          AND table_schema = SPLIT(p_source_table_name, '.') [OFFSET(1)]
          AND table_catalog = SPLIT(p_source_table_name, '.') [OFFSET(0)]
          AND column_name NOT IN (p_start_date_column, p_end_date_column, p_is_current_column) -- Exclude SCD specific columns
    );

    -- Create a temporary table to hold the records that need to be expired or inserted.
    -- This intermediate step helps in managing the merge logic more clearly and efficiently.
    SET v_temp_table_name = FORMAT('%s_scd_temp_%s', REPLACE(REPLACE(p_target_table_name, '.', '_'), '-', '_'), GENERATE_UUID());

    EXECUTE IMMEDIATE FORMAT("""
        CREATE TEMPORARY TABLE `%s` AS
        SELECT
            S.*,
            -- Determine if a record is new or changed
            CASE
                WHEN T.%s IS NULL THEN 'NEW' -- Record exists in source but not in target (based on current active records)
                WHEN (%s) THEN 'CHANGED'     -- Record exists in both, but change columns differ
                ELSE 'NO_CHANGE'             -- Record exists and no relevant changes
            END AS __scd_action
        FROM
            `%s` AS S
        LEFT JOIN
            `%s` AS T
        ON
            %s AND T.%s = TRUE
        -- If a timestamp column is provided, order by it to handle multiple updates within a batch correctly.
        -- This ensures that if a record is updated multiple times in the source, the latest version is considered.
        QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) = 1
        ;
    """,
    v_temp_table_name,
    SPLIT(v_key_columns_array[OFFSET(0)], ' ')[OFFSET(0)], -- Use first key column for NULL check
    v_change_detection_condition,
    p_source_table_name,
    p_target_table_name,
    v_key_join_condition,
    p_is_current_column,
    p_key_columns_csv, -- Partition by all key columns
    CASE WHEN p_timestamp_column IS NOT NULL THEN p_timestamp_column ELSE '1' END -- Order by timestamp or a constant if not provided
    );

    -- Step 1: Expire old records (Set end_date and is_current = FALSE)
    -- This updates records in the target table that correspond to 'CHANGED' records
    -- identified in the temporary table.
    EXECUTE IMMEDIATE FORMAT("""
        UPDATE `%s` AS T
        SET
            T.%s = '%s', -- Set end date to current timestamp
            T.%s = FALSE -- Mark as not current
        FROM
            `%s` AS Temp
        WHERE
            %s AND T.%s = TRUE
            AND Temp.__scd_action = 'CHANGED'
        ;
    """,
    p_target_table_name,
    p_end_date_column,
    v_current_timestamp,
    p_is_current_column,
    v_temp_table_name,
    v_key_join_condition,
    p_is_current_column
    );

    -- Step 2: Insert new records and new versions of changed records
    -- This inserts records from the temporary table that are either 'NEW' or 'CHANGED'.
    EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `%s` (
            %s,
            %s,
            %s,
            %s
        )
        SELECT
            %s,
            '%s', -- Set start date to current timestamp
            NULL, -- End date is initially NULL for current records
            TRUE  -- Mark as current
        FROM
            `%s` AS Temp
        WHERE
            Temp.__scd_action IN ('NEW', 'CHANGED')
        ;
    """,
    p_target_table_name,
    v_all_source_columns,
    p_start_date_column,
    p_end_date_column,
    p_is_current_column,
    v_all_source_columns,
    v_current_timestamp,
    v_temp_table_name
    );

    -- Clean up the temporary table
    EXECUTE IMMEDIATE FORMAT("DROP TEMPORARY TABLE `%s`;", v_temp_table_name);

    -- Log success
    SELECT FORMAT('SCD Type 2 merge completed for target table: %s', p_target_table_name) AS status;

EXCEPTION WHEN ERROR THEN
    -- Log error and re-raise
    SELECT FORMAT('Error during SCD Type 2 merge for target table %s: %s', p_target_table_name, @@error.message) AS error_message;
    RAISE;
END;
