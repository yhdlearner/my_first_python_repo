import os
import time
from google.cloud import bigquery, storage

def bq_to_gcs_export(
    input_path: str,
    output_gcs_path: str,
    temp_dataset: str = None,
    delimiter: str = ',',
    header_flag: bool = True,
    compose_sleep_seconds: int = 5
):
    """
    A common process to export data from BigQuery to a single delimited file in GCS.

    This function can take either a SQL file or a BigQuery table name as input.
    It handles exporting to multiple files and composing them into a single file,
    robustly managing the 32-file composition limit of Google Cloud Storage.

    Args:
        input_path (str): The path to a SQL file or a BigQuery table name
                          in the format 'project.dataset.table'.
        output_gcs_path (str): The destination GCS path for the final single file,
                               e.g., 'gs://your-bucket/output/data.csv'.
        temp_dataset (str): The temporary BigQuery dataset to use if the input
                            is a SQL file. Required for SQL file inputs.
        delimiter (str): The delimiter for the output file. Defaults to ','.
        header_flag (bool): Whether to include a header row. Defaults to True.
        compose_sleep_seconds (int): The number of seconds to sleep between
                                     each file composition step. Defaults to 5.
    """

    bq_client = bigquery.Client()
    storage_client = storage.Client()

    temp_table_id = None
    destination_table_ref = None

    try:
        # --- Step 1: Determine the source table for the export ---
        if input_path.lower().endswith('.sql'):
            # Handle SQL file input
            if not temp_dataset:
                raise ValueError("temp_dataset must be provided for SQL file inputs.")

            # Generate a unique temporary table ID
            temp_table_id = f"temp_export_table_{os.path.basename(input_path).replace('.sql', '')}_{bq_client.project}_{os.urandom(4).hex()}"
            destination_table_ref = bq_client.dataset(temp_dataset).table(temp_table_id)

            print(f"Executing SQL from {input_path} into temporary table: {destination_table_ref.path}")
            
            # Read the SQL file content
            with open(input_path, 'r') as file:
                sql_query = file.read()

            # Configure and run the query job
            job_config = bigquery.QueryJobConfig(destination=destination_table_ref, write_disposition='WRITE_TRUNCATE')
            query_job = bq_client.query(sql_query, job_config=job_config)
            query_job.result()  # Wait for the job to complete

            source_table_ref = destination_table_ref
            print("Query execution completed successfully.")

        else:
            # Handle table name input
            source_table_ref = bigquery.Table.from_string(input_path)
            print(f"Using existing BigQuery table: {source_table_ref.path}")

        # --- Step 2: Extract data from BigQuery to GCS ---
        bucket_name = output_gcs_path.replace('gs://', '').split('/')[0]
        # Use a wildcard to handle multi-file exports
        destination_uri = f"gs://{bucket_name}/{'/'.join(output_gcs_path.replace('gs://', '').split('/')[1:])}_part_*.csv"
        
        print(f"Extracting data to temporary GCS files at: {destination_uri}")

        # Configure the extract job
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.CSV,
            field_delimiter=delimiter,
            print_header=header_flag,
        )

        extract_job = bq_client.extract_table(
            source_table_ref,
            destination_uri,
            job_config=job_config,
        )
        extract_job.result()  # Wait for the job to complete
        print("Data extraction completed successfully.")

        # --- Step 3: Compose the files into a single output file ---
        final_blob_path = '/'.join(output_gcs_path.replace('gs://', '').split('/')[1:])
        final_blob = storage_client.bucket(bucket_name).blob(final_blob_path)
        
        # Get all the part files created by the export job
        temp_blob_prefix = final_blob_path + '_part_'
        part_blobs = list(storage_client.bucket(bucket_name).list_blobs(prefix=temp_blob_prefix))

        if not part_blobs:
            print("No files to compose, export may have failed or resulted in an empty file.")
            return

        if len(part_blobs) == 1:
            # If only one file was created, rename it to the final name
            print("Only one file created, renaming it to the final output file.")
            storage_client.bucket(bucket_name).rename_blob(part_blobs[0], final_blob_path)
        else:
            # Robustly handle composition for more than 32 files
            print(f"Found {len(part_blobs)} files to compose. Starting chained composition.")
            
            # Use chunks of 32 for composition
            source_blobs = [storage_client.bucket(bucket_name).get_blob(blob.name) for blob in part_blobs]
            
            # The first chunk creates a temporary composed file
            temp_composed_blob = source_blobs[0]
            
            # Start from the second chunk
            remaining_blobs_to_compose = source_blobs[1:]

            while remaining_blobs_to_compose:
                # Take the next 31 blobs and the last composed blob
                blobs_to_compose_now = [temp_composed_blob] + remaining_blobs_to_compose[:31]
                
                # Compose them into a new temporary blob
                new_temp_composed_blob_name = f"{temp_blob_prefix}temp_composed_{os.urandom(4).hex()}"
                new_temp_composed_blob = storage_client.bucket(bucket_name).blob(new_temp_composed_blob_name)
                
                print(f"Composing {len(blobs_to_compose_now)} blobs into {new_temp_composed_blob_name}")
                new_temp_composed_blob.compose(blobs_to_compose_now)

                # Delete the blobs that were just composed, except the original temp_composed_blob for the first iteration
                for blob in blobs_to_compose_now:
                    blob.delete()
                
                # The new composed file becomes the source for the next iteration
                temp_composed_blob = new_temp_composed_blob
                
                # Update the list of remaining blobs
                remaining_blobs_to_compose = remaining_blobs_to_compose[31:]

                # Wait for the specified duration to avoid API errors
                if remaining_blobs_to_compose:
                    print(f"Sleeping for {compose_sleep_seconds} seconds before next composition step...")
                    time.sleep(compose_sleep_seconds)

            # The final composed blob is the last temp_composed_blob
            temp_composed_blob.rename(final_blob_path)
            print(f"Composition completed. Final file is at: {output_gcs_path}")

    finally:
        # --- Step 4: Cleanup temporary table ---
        if temp_table_id and bq_client.dataset(temp_dataset).table(temp_table_id) in bq_client.list_tables(temp_dataset):
            print(f"Cleaning up temporary BigQuery table: {temp_table_id}")
            bq_client.delete_table(destination_table_ref, not_found_ok=True)
