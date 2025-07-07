CREATE OR REPLACE PROCEDURE `your_project.your_dataset.scd_type2_procedure`(
    source_table_name STRING,         -- Fully qualified source table name (e.g., '`project.dataset.source_table`')
    target_table_name STRING,         -- Fully qualified target table name (e.g., '`project.dataset.target_table`')
    key_columns ARRAY<STRING>,        -- Array of column names that uniquely identify a record (e.g., ['customer_id'])
    change_columns ARRAY<STRING>,     -- Array of column names that, if changed, trigger a new version (e.g., ['address', 'phone_number'])
    effective_start_column STRING,    -- Name of the 'effective start date' column in the target table (e.g., 'valid_from')
    effective_end_column STRING,      -- Name of the 'effective end date' column in the target table (e.g., 'valid_to')
    is_current_column STRING,         -- Name of the 'is current' boolean flag column in the target table (e.g., 'is_active')
    high_date_value STRING            -- A string representing the 'high date' for current records (e.g., '9999-12-31 23:59:59')
)
BEGIN
    -- Declare variables to hold dynamic SQL components
    DECLARE sql_statement STRING;
    DECLARE key_join_condition STRING;
    DECLARE change_detection_condition STRING;
    DECLARE source_columns_list STRING;

    -- Extract project, dataset, and table names from the fully qualified source table name
    DECLARE project_id STRING;
    DECLARE dataset_id STRING;
    DECLARE table_id STRING;

    SET project_id = SPLIT(source_table_name, '.')[OFFSET(0)];
    SET dataset_id = SPLIT(source_table_name, '.')[OFFSET(1)];
    SET table_id = SPLIT(source_table_name, '.')[OFFSET(2)];

    -- 1. Construct the JOIN condition for key columns
    -- Example: S.`customer_id` = T.`customer_id` AND S.`order_id` = T.`order_id`
    SET key_join_condition = (
        SELECT STRING_AGG(FORMAT('S.`%s` = T.`%s`', col, col), ' AND ')
        FROM UNNEST(key_columns) AS col
    );

    -- 2. Construct the change detection condition
    -- Uses IS DISTINCT FROM to handle NULL comparisons correctly.
    -- Example: S.`address` IS DISTINCT FROM T.`address` OR S.`phone_number` IS DISTINCT FROM T.`phone_number`
    SET change_detection_condition = (
        SELECT STRING_AGG(FORMAT('S.`%s` IS DISTINCT FROM T.`%s`', col, col), ' OR ')
        FROM UNNEST(change_columns) AS col
    );

    -- 3. Dynamically get all column names from the source table for the INSERT statement.
    -- This ensures that the INSERT statement includes all original source columns.
    -- We exclude the SCD-specific columns, as they are added by the procedure.
    EXECUTE IMMEDIATE FORMAT("""
        SELECT STRING_AGG(FORMAT('`%%s`', column_name), ', ')
        FROM `%s`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_catalog = '%s'
          AND table_schema = '%s'
          AND table_name = '%s'
          AND column_name NOT IN ('%s', '%s', '%s') -- Exclude SCD specific columns
    """, project_id, project_id, dataset_id, table_id, effective_start_column, effective_end_column, is_current_column)
    INTO source_columns_list;

    -- Start a transaction to ensure atomicity of the update and insert operations
    BEGIN TRANSACTION;

    -- Step 1: Update (close out) existing current records in the target table
    -- These are records that exist in the target, are marked as current,
    -- have a matching key in the source, but their 'change_columns' values have changed.
    SET sql_statement = FORMAT("""
        UPDATE `%s` AS T
        SET
            `%s` = CURRENT_TIMESTAMP(), -- Set end date to current timestamp
            `%s` = FALSE                -- Mark as not current
        FROM
            `%s` AS S
        WHERE
            T.`%s` = TRUE               -- Only update currently active records
            AND %s                      -- Join on key columns
            AND %s;                     -- Check for changes in specified columns
    """,
        target_table_name,
        effective_end_column,
        is_current_column,
        source_table_name,
        is_current_column,
        key_join_condition,
        change_detection_condition
    );
    EXECUTE IMMEDIATE sql_statement;

    -- Step 2: Insert new records into the target table
    -- This includes two types of records:
    --   a) Truly new records from the source (no matching key in the target's current records).
    --   b) New versions of records that were just updated (closed out) in Step 1 due to changes.
    SET sql_statement = FORMAT("""
        INSERT INTO `%s` (%s, `%s`, `%s`, `%s`)
        SELECT
            %s,                                         -- All original columns from the source
            CURRENT_TIMESTAMP() AS `%s`,                -- Set effective start date to current timestamp
            PARSE_TIMESTAMP('%%%%Y-%%%%m-%%%%d %%%%H:%%%%M:%%%%S', '%s') AS `%s`, -- Set effective end date to high date
            TRUE AS `%s`                                -- Mark as current
        FROM
            `%s` AS S
        LEFT JOIN
            `%s` AS T
        ON
            %s                                          -- Join on key columns
            AND T.`%s` = TRUE                           -- Only consider current records in target for join
        WHERE
            T.`%s` IS NULL                              -- Condition for truly new records (no current match in target)
            OR (
                T.`%s` IS NOT NULL AND %s               -- Condition for new versions of changed records
            );
    """,
        target_table_name,
        source_columns_list,
        effective_start_column,
        effective_end_column,
        is_current_column,
        source_columns_list,
        effective_start_column,
        high_date_value,
        effective_end_column,
        is_current_column,
        source_table_name,
        target_table_name,
        key_join_condition,
        is_current_column,
        key_columns[OFFSET(0)], -- Use the first key column for IS NULL check
        key_columns[OFFSET(0)], -- Use the first key column for IS NOT NULL check
        change_detection_condition
    );
    EXECUTE IMMEDIATE sql_statement;

    -- Optional: Handle records that are present in the target but no longer in the source.
    -- This marks them as not current. Uncomment if you need to manage deletions as part of SCD Type 2.
    /*
    SET sql_statement = FORMAT("""
        UPDATE `%s` AS T
        SET
            `%s` = CURRENT_TIMESTAMP(),
            `%s` = FALSE
        WHERE
            T.`%s` = TRUE
            AND NOT EXISTS (
                SELECT 1
                FROM `%s` AS S
                WHERE %s
            );
    """,
        target_table_name,
        effective_end_column,
        is_current_column,
        is_current_column,
        source_table_name,
        -- This join condition needs to be adapted for the NOT EXISTS subquery.
        -- It should compare T's keys with S's keys directly within the subquery.
        -- Example: STRING_AGG(FORMAT('S.`%s` = T.`%s`', col, col), ' AND ')
        (SELECT STRING_AGG(FORMAT('S.`%s` = T.`%s`', col, col), ' AND ') FROM UNNEST(key_columns) AS col)
    );
    EXECUTE IMMEDIATE sql_statement;
    */

    -- Commit the transaction
    COMMIT TRANSACTION;

END;



