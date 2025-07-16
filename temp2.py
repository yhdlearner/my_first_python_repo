from __future__ import annotations

import pendulum
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# Import Google Cloud Storage client library
from google.cloud import storage

# Set up logging
log = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5),
}

# --- Configuration Variables ---
# Replace these with your actual GCP project, dataset, bucket, and table names
GCP_PROJECT_ID = 'your-gcp-project-id'
BQ_DATASET_ID = 'your_dataset_id'
BQ_TABLE_NAME = 'dim_customers' # Your SCD Type 2 dimension table
GCS_BUCKET_NAME = 'your-gcs-bucket-name'
GCS_FILE_PREFIX = 'fixed_length_exports/customers' # Path within the bucket
# -------------------------------

# Define the fixed-length schema for your output file.
# Each dictionary represents a column:
# 'name': BigQuery column name
# 'length': Desired fixed length in the output file
# 'padding_char': Character to use for padding (e.g., ' ' for space, '0' for zero)
# 'alignment': 'left' (RPAD) or 'right' (LPAD)
# 'data_type': BigQuery data type (used for casting before padding, e.g., 'STRING', 'INT64', 'FLOAT64')
# 'null_representation': How NULLs should appear (e.g., '', '0', 'N/A'). This will be padded.
FIXED_LENGTH_SCHEMA = [
    {'name': 'customer_id', 'length': 10, 'padding_char': ' ', 'alignment': 'right', 'data_type': 'STRING', 'null_representation': ''},
    {'name': 'customer_name', 'length': 50, 'padding_char': ' ', 'alignment': 'right', 'data_type': 'STRING', 'null_representation': ''},
    {'name': 'address', 'length': 100, 'padding_char': ' ', 'alignment': 'right', 'data_type': 'STRING', 'null_representation': ''},
    {'name': 'city', 'length': 30, 'padding_char': ' ', 'alignment': 'right', 'data_type': 'STRING', 'null_representation': ''},
    {'name': 'state', 'length': 2, 'padding_char': ' ', 'alignment': 'right', 'data_type': 'STRING', 'null_representation': ''},
    {'name': 'zip_code', 'length': 5, 'padding_char': ' ', 'alignment': 'right', 'data_type': 'STRING', 'null_representation': ''},
    {'name': 'email', 'length': 70, 'padding_char': ' ', 'alignment': 'right', 'data_type': 'STRING', 'null_representation': ''},
    # Add more columns as needed based on your dim_customers table
]

def _generate_fixed_length_sql(
    project_id: str,
    dataset_id: str,
    table_name: str,
    schema_definition: list[dict],
    gcs_bucket: str,
    gcs_file_prefix: str,
    ds: str, # Airflow's execution date (YYYY-MM-DD)
    run_id: str, # Airflow's run ID, useful for unique identifiers
    **kwargs
) -> str:
    """
    Generates the BigQuery SQL query to create a fixed-length string for each record
    and export it directly to GCS.
    Pushes the generated GCS URI prefix and a unique ID (from run_id) to XComs.
    """
    select_parts = []
    for field in schema_definition:
        col_name = field['name']
        length = field['length']
        padding_char = field['padding_char']
        alignment = field['alignment']
        data_type = field['data_type']
        null_rep = field['null_representation']

        # Cast to STRING first to handle various data types consistently
        # Use COALESCE to handle NULLs, replacing them with the defined null_representation
        # Then apply padding based on alignment
        if alignment == 'right':
            padded_col = f"RPAD(COALESCE(CAST({col_name} AS STRING), '{null_rep}'), {length}, '{padding_char}')"
        elif alignment == 'left':
            padded_col = f"LPAD(COALESCE(CAST({col_name} AS STRING), '{null_rep}'), {length}, '{padding_char}')"
        else:
            raise ValueError(f"Unsupported alignment: {alignment}. Must be 'left' or 'right'.")

        select_parts.append(padded_col)

    # Concatenate all padded parts into a single string
    fixed_length_record_expression = "CONCAT(\n    " + ",\n    ".join(select_parts) + "\n)"

    # Use a unique identifier for the temporary sharded files.
    # We'll use the Airflow run_id or a portion of it.
    unique_export_id = run_id.replace('manual__', '').replace('scheduled__', '').replace('__', '_')

    # Construct the full EXPORT DATA query
    # Use a wildcard '*' in the URI to indicate BigQuery can write multiple sharded files.
    gcs_temp_output_uri = f"gs://{gcs_bucket}/{gcs_file_prefix}/temp_{unique_export_id}_customers_fixed_length_*.txt"

    sql_query = f"""
        EXPORT DATA OPTIONS(
            uri='{gcs_temp_output_uri}',
            format='CSV',
            overwrite=true,
            field_delimiter='', -- Crucial for fixed-length output
            header=false        -- Crucial for raw fixed-length output
        ) AS
        SELECT
            {fixed_length_record_expression} AS fixed_length_record
        FROM
            `{project_id}.{dataset_id}.{table_name}`
        WHERE
            is_current = TRUE; -- Assuming you want only current records from your SCD Type 2 table
    """
    log.info(f"Generated BigQuery SQL:\n{sql_query}")

    # Push necessary variables to XCom for the next task
    kwargs['ti'].xcom_push(key='fixed_length_export_sql', value=sql_query)
    kwargs['ti'].xcom_push(key='unique_export_id', value=unique_export_id)
    kwargs['ti'].xcom_push(key='gcs_bucket_name', value=gcs_bucket)
    kwargs['ti'].xcom_push(key='gcs_file_prefix', value=gcs_file_prefix)

def _compose_gcs_files(gcs_bucket_name: str, gcs_file_prefix: str, unique_export_id: str):
    """
    Composes multiple GCS files into a single file, handling the 32-object limit
    of gsutil compose (and underlying GCS API).
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)

    source_blob_prefix = f"{gcs_file_prefix}/temp_{unique_export_id}_customers_fixed_length_"
    final_dest_blob_name = f"{gcs_file_prefix}/final_{unique_export_id}_customers_fixed_length.txt"

    log.info(f"Starting iterative composition for files with prefix: {source_blob_prefix}")

    # List all source blobs that need to be composed
    # Filter by prefix to get only the relevant temporary files
    temp_blobs = list(bucket.list_blobs(prefix=source_blob_prefix))
    temp_blobs = [blob for blob in temp_blobs if blob.name.startswith(source_blob_prefix) and blob.name.endswith('.txt')]

    num_files = len(temp_blobs)
    log.info(f"Found {num_files} temporary files to compose.")

    if num_files == 0:
        log.info("No files found to compose. Exiting gracefully.")
        return # Exit Python function

    # If 32 or fewer files, compose directly
    if num_files <= 32:
        log.info(f"Number of files ({num_files}) is 32 or less. Composing directly.")
        # Create a new blob object for the destination
        destination_blob = bucket.blob(final_dest_blob_name)
        destination_blob.compose(temp_blobs)
        log.info(f"Composition complete: gs://{gcs_bucket_name}/{final_dest_blob_name}")

        # Remove the source files after successful composition
        for blob in temp_blobs:
            blob.delete()
            log.info(f"Deleted temporary file: {blob.name}")
        return

    # --- Iterative Composition for > 32 files ---
    log.info(f"Number of files ({num_files}) is greater than 32. Performing iterative composition.")

    current_blobs = list(temp_blobs) # Make a mutable copy
    iteration = 0

    # Loop until only one file (the final composite) remains
    while len(current_blobs) > 1:
        iteration += 1
        log.info(f"--- Iteration {iteration} ---")
        new_composed_blobs = [] # To store the results of this iteration's compositions
        num_current_files = len(current_blobs)

        # Process blobs in chunks of 32
        for i in range(0, num_current_files, 32):
            chunk_blobs = current_blobs[i : i + 32]

            # Generate a unique name for the intermediate composite file for this chunk
            intermediate_blob_name = (
                f"{gcs_file_prefix}/intermediate_{unique_export_id}_iter{iteration}_chunk{i}.txt"
            )
            log.info(f"Composing chunk of {len(chunk_blobs)} files into: {intermediate_blob_name}")

            # Perform the compose operation for the current chunk
            intermediate_blob = bucket.blob(intermediate_blob_name)
            intermediate_blob.compose(chunk_blobs)

            # Add the newly composed intermediate blob to the list for the next iteration
            new_composed_blobs.append(intermediate_blob)

            # Remove the source blobs that were just composed in this chunk
            log.info(f"Removing {len(chunk_blobs)} source files for this chunk...")
            for blob in chunk_blobs:
                blob.delete()
                log.info(f"Deleted temporary file: {blob.name}")

        # Update current_blobs for the next iteration
        current_blobs = new_composed_blobs
        log.info(f"Number of files remaining for next iteration: {len(current_blobs)}")

    # After the loop, current_blobs should contain exactly one blob, which is the final composite.
    final_composite_blob = current_blobs[0]
    log.info(f"Final composite file created at intermediate stage: {final_composite_blob.name}")

    # Rename the final intermediate blob to the desired target filename
    # This handles cases where the last composition might have created a new intermediate file
    # rather than directly naming it the final target.
    if final_composite_blob.name != final_dest_blob_name:
        log.info(f"Renaming final composite file from {final_composite_blob.name} to {final_dest_blob_name}")
        final_composite_blob.rename(final_dest_blob_name)

    log.info(f"All temporary files have been composed into a single file: gs://{gcs_bucket_name}/{final_dest_blob_name}")


with DAG(
    dag_id='bigquery_fixed_length_export_to_gcs',
    default_args=default_args,
    description='Exports current BigQuery dimension table data as a single fixed-length file to GCS using iterative compose.',
    schedule=pendulum.duration(days=1), # Run daily
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['bigquery', 'gcs', 'export', 'fixed-length', 'scd', 'compose'],
) as dag:
    generate_sql_task = PythonOperator(
        task_id='generate_fixed_length_export_sql',
        python_callable=_generate_fixed_length_sql,
        op_kwargs={
            'project_id': GCP_PROJECT_ID,
            'dataset_id': BQ_DATASET_ID,
            'table_name': BQ_TABLE_NAME,
            'schema_definition': FIXED_LENGTH_SCHEMA,
            'gcs_bucket': GCS_BUCKET_NAME,
            'gcs_file_prefix': GCS_FILE_PREFIX,
        },
    )

    execute_bq_export_task = BigQueryExecuteQueryOperator(
        task_id='execute_bigquery_fixed_length_export',
        sql="{{ task_instance.xcom_pull(task_ids='generate_fixed_length_export_sql', key='fixed_length_export_sql') }}",
        use_legacy_sql=False, # Use standard SQL
        location='US', # Specify your BigQuery dataset location (e.g., 'US', 'EU')
        # Ensure the service account running the Airflow worker has necessary permissions
        # (BigQuery Job User, BigQuery Data Viewer, Storage Object Creator)
    )

    compose_gcs_files_task = PythonOperator(
        task_id='compose_fixed_length_file',
        python_callable=_compose_gcs_files,
        op_kwargs={
            'gcs_bucket_name': "{{ task_instance.xcom_pull(task_ids='generate_fixed_length_export_sql', key='gcs_bucket_name') }}",
            'gcs_file_prefix': "{{ task_instance.xcom_pull(task_ids='generate_fixed_length_export_sql', key='gcs_file_prefix') }}",
            'unique_export_id': "{{ task_instance.xcom_pull(task_ids='generate_fixed_length_export_sql', key='unique_export_id') }}",
        },
        # Set a longer timeout for this task as it might take time for many files
        execution_timeout=pendulum.duration(minutes=30)
    )

    # Define task dependencies
    generate_sql_task >> execute_bq_export_task >> compose_gcs_files_task
