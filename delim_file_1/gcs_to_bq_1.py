import logging
import uuid
from pathlib import Path
from typing import Optional

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def export_bigquery_to_gcs_with_compose(
    input_source: str,
    gcs_bucket: str,
    output_filename: str,
    gcp_conn_id: str = 'google_cloud_default',
    temp_dataset: Optional[str] = None,
    delimiter: str = ',',
    print_header: bool = True,
):
    """
    Exports data from BigQuery to a single delimited file in a GCS bucket.

    This function handles two types of inputs: a direct BigQuery table name or a
    SQL file. It manages the creation of temporary tables for SQL queries and
    efficiently composes multiple sharded output files from the BigQuery export
    into a single destination file using the GCS compose API. It is designed to
    be used as a Python callable in an Airflow DAG.

    :param input_source: The source of the data. Can be a BigQuery table in
        'dataset.table' format or a local path to a .sql file.
    :param gcs_bucket: The GCS bucket to upload the final file to.
    :param output_filename: The desired name for the final, single output file
        in the GCS bucket.
    :param gcp_conn_id: The Airflow connection ID for Google Cloud.
    :param temp_dataset: (Optional) The BigQuery dataset to use for storing
        temporary tables when the input is a SQL file. Required if input_source
        is a .sql file.
    :param delimiter: The delimiter to use for the output file.
    :param print_header: Whether to include a header row in the output file.
    """
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    project_id = bq_hook.project_id
    source_table_id = None
    temp_table_created = False

    # A temporary GCS prefix to store the sharded files from the BQ export
    temp_gcs_prefix = f"temp_compose/{uuid.uuid4()}/"

    try:
        # Step 1: Determine input source and prepare the source table
        if input_source.lower().endswith('.sql'):
            logger.info(f"Input is a SQL file: {input_source}")
            if not temp_dataset:
                raise ValueError("`temp_dataset` must be provided for SQL file inputs.")

            sql = Path(input_source).read_text()
            temp_table_id = f"{project_id}.{temp_dataset}.temp_{uuid.uuid4().hex}"
            logger.info(f"Executing SQL and creating temporary table: {temp_table_id}")

            bq_hook.run_query(
                sql=sql,
                destination_dataset_table=temp_table_id,
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
            )
            source_table_id = temp_table_id
            temp_table_created = True
        else:
            logger.info(f"Input is a BigQuery table: {input_source}")
            source_table_id = f"{project_id}.{input_source}"

        # Step 2: Export the table from BigQuery to GCS
        # The export job will create multiple sharded files (part-*)
        export_destination_uri = f"gs://{gcs_bucket}/{temp_gcs_prefix}part-*"
        logger.info(f"Exporting table {source_table_id} to {export_destination_uri}")

        bq_hook.run_extract(
            source_project_dataset_table=source_table_id,
            destination_cloud_storage_uris=[export_destination_uri],
            field_delimiter=delimiter,
            print_header=print_header,
        )

        # Step 3: Compose the sharded files into a single file
        logger.info("Composing sharded files into a single file.")
        files_to_compose = gcs_hook.list(bucket_name=gcs_bucket, prefix=temp_gcs_prefix)

        if not files_to_compose:
            raise RuntimeError("BigQuery export job created no files.")
        
        if len(files_to_compose) == 1:
            logger.info("Only one file was created; renaming it to the final destination.")
            source_object = files_to_compose[0]
            gcs_hook.rewrite(
                source_bucket=gcs_bucket,
                source_object=source_object,
                destination_bucket=gcs_bucket,
                destination_object=output_filename,
            )
        else:
            # This loop handles composing more than 32 files by creating
            # intermediate composed files and then composing those intermediates.
            source_files = list(files_to_compose)
            while len(source_files) > 1:
                logger.info(f"Composition loop: {len(source_files)} files remaining.")
                target_files_for_next_iteration = []
                
                # Process in chunks of 32
                for i in range(0, len(source_files), 32):
                    chunk = source_files[i:i+32]
                    
                    is_final_compose = (len(source_files) <= 32)
                    if is_final_compose:
                        destination_object_name = output_filename
                    else:
                        # Create an intermediate composed file
                        destination_object_name = f"{temp_gcs_prefix}composed-{uuid.uuid4().hex}"
                        target_files_for_next_iteration.append(destination_object_name)

                    logger.info(f"Composing {len(chunk)} files into {destination_object_name}")
                    gcs_hook.compose(
                        bucket_name=gcs_bucket,
                        source_objects=chunk,
                        destination_object=destination_object_name,
                    )
                
                # The source for the next iteration will be the newly created composed files
                source_files = target_files_for_next_iteration

    finally:
        # Step 4: Cleanup
        logger.info("Starting cleanup process.")
        try:
            sharded_files = gcs_hook.list(bucket_name=gcs_bucket, prefix=temp_gcs_prefix)
            if sharded_files:
                logger.info(f"Deleting {len(sharded_files)} temporary files from GCS.")
                gcs_hook.delete(bucket_name=gcs_bucket, objects=sharded_files)
        except Exception as e:
            logger.error(f"Error during GCS cleanup: {e}", exc_info=True)

        if temp_table_created and source_table_id:
            try:
                logger.info(f"Deleting temporary BigQuery table: {source_table_id}")
                bq_hook.run_table_delete(table_id=source_table_id)
            except Exception as e:
                logger.error(f"Error deleting temp BQ table {source_table_id}: {e}", exc_info=True)

    logger.info(f"Process complete. Final file is at gs://{gcs_bucket}/{output_filename}")


# --- Example of how to use this in an Airflow DAG ---
# from __future__ import annotations
#
# import pendulum
# from airflow.models.dag import DAG
# from airflow.operators.python import PythonOperator
#
# # Assuming the function above is in a file named 'bq_to_gcs_export.py'
# # from your_utils_folder.bq_to_gcs_export import export_bigquery_to_gcs_with_compose
#
# with DAG(
#     dag_id='example_bq_to_gcs_compose_export',
#     start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
#     catchup=False,
#     schedule=None,
#     tags=['gcs', 'bigquery', 'example', 'compose'],
# ) as dag:
#     # Example: Using a SQL file as input
#     export_from_sql_task = PythonOperator(
#         task_id='export_from_sql_and_compose',
#         python_callable=export_bigquery_to_gcs_with_compose,
#         op_kwargs={
#             'input_source': '/path/to/your/dags/sqls/get_large_dataset.sql',
#             'gcs_bucket': 'your-gcs-bucket-name',
#             'output_filename': 'exports/large_dataset_from_sql.csv',
#             'temp_dataset': 'your_temp_dataset', # Important for SQL inputs
#             'delimiter': ',',
#             'print_header': True,
#         },
#     )
