from airflow.operators.bash import BashOperator

# ... (previous DAG setup) ...

compose_gcs_files_task = BashOperator(
    task_id='compose_fixed_length_file',
    bash_command="""
        GCS_BUCKET="{{ params.gcs_bucket_name }}"
        GCS_TEMP_PATH="{{ params.gcs_file_prefix }}"
        SESSION_ID="{{ task_instance.xcom_pull(task_ids='generate_fixed_length_export_sql', key='session_id') }}" # Assuming you push session_id via XCom

        SOURCE_FILES="gs://${GCS_BUCKET}/${GCS_TEMP_PATH}temp_${SESSION_ID}_customers_fixed_length_*.txt"
        DEST_FILE="gs://${GCS_BUCKET}/${GCS_TEMP_PATH}final_${SESSION_ID}_customers_fixed_length.txt"

        gsutil compose ${SOURCE_FILES} ${DEST_FILE} && gsutil rm ${SOURCE_FILES}
    """,
    params={
        'gcs_bucket_name': 'your-gcs-bucket-name',
        'gcs_file_prefix': 'fixed_length_exports/customers/'
    }
)

# Define dependencies: BigQuery export must complete before composing
# execute_bq_export_task >> compose_gcs_files_task