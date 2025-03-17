from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import io
import os

ADMIN_EMAIL = "admin@example.com"

def task_success_log(context):
    """Callback function to log task success."""
    task_instance = context.get("task_instance")
    if task_instance:
        print(f"Task {task_instance.task_id} completed successfully.")

@task
def upload_folder_to_s3(local_folder: str, s3_bucket: str, s3_prefix: str) -> None:
    """
    Upload all files from a local folder (and its subfolders) to S3.
    """
    s3 = S3Hook()
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_file = os.path.join(root, file)
            # Build the S3 key relative to the local folder
            relative_path = os.path.relpath(local_file, local_folder)
            s3_key = os.path.join(s3_prefix, relative_path)
            s3.load_file(filename=local_file, key=s3_key, bucket_name=s3_bucket, replace=True)
            print(f"Uploaded file {local_file} to s3://{s3_bucket}/{s3_key}")
    print(f"Finished uploading all files from folder '{local_folder}' to s3://{s3_bucket}/{s3_prefix}")

@task
def validate_csv_columns(s3_bucket: str, s3_key: str, required_columns: list[str]) -> bool:
    """
    Validate if the CSV file in the S3 bucket contains the required columns.
    """
    s3 = S3Hook()
    file_obj = s3.get_key(key=s3_key, bucket_name=s3_bucket)
    file_content = file_obj.get()["Body"].read().decode("utf-8")
    csv_data = pd.read_csv(io.StringIO(file_content))
    if all(column in csv_data.columns for column in required_columns):
        print("CSV validation passed: All required columns are present.")
        return True
    else:
        print("CSV validation failed: Missing required columns.")
        return False

@task.branch
def branch_validation(valid: bool) -> str:
    """
    Branch the DAG based on CSV validation results.
    Returns the task id to follow.
    """
    if valid:
        print("Validation successful. Proceeding with Glue job execution.")
        return "run_glue_job"
    else:
        print("Validation unsuccessful. Terminating DAG execution.")
        return "validation_failed"

@task
def load_to_dynamodb(processed_data_s3_bucket: str, processed_data_s3_key: str, dynamodb_table_name: str):
    """
    Load processed CSV data from S3 into a DynamoDB table.
    """
    s3 = S3Hook()
    file_obj = s3.get_key(key=processed_data_s3_key, bucket_name=processed_data_s3_bucket)
    file_content = file_obj.get()["Body"].read().decode("utf-8")
    csv_data = pd.read_csv(io.StringIO(file_content))
    
    dynamodb = DynamoDBHook()
    table = dynamodb.get_conn().Table(dynamodb_table_name)
    
    with table.batch_writer() as batch:
        for _, row in csv_data.iterrows():
            batch.put_item(Item=row.to_dict())
    print("Data loaded into DynamoDB successfully.")

@dag(
    dag_id="s3_to_dynamodb_with_glue",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "email": [ADMIN_EMAIL],
        "email_on_failure": True
    },
    description="DAG to upload folder data to S3, validate CSV, process with Glue, and load into DynamoDB.",
    tags=["S3", "Glue", "DynamoDB"]
)
def s3_to_dynamodb_with_glue():
    # Define variables
    local_folder_path = "<local_folder_path>"          # Local folder containing CSV files
    source_s3_bucket = "<source_s3_bucket>"
    s3_upload_prefix = "<source_s3_prefix>"              # S3 prefix (folder) for uploads
    archive_s3_bucket = "<archive_s3_bucket>"
    processed_data_s3_bucket = "<processed_data_s3_bucket>"
    processed_data_s3_key = "<processed_data_s3_key>"     # Processed data file key in S3
    dynamodb_table_name = "<dynamodb_table_name>"
    glue_job_name = "<glue_job_name>"
    required_columns = ["user_id", "track_id", "listen_time"]
    
    # 1. Upload all files from the local folder to S3
    upload_files = upload_folder_to_s3(
        local_folder=local_folder_path,
        s3_bucket=source_s3_bucket,
        s3_prefix=s3_upload_prefix
    )
    
    # 2. Validate CSV columns from a specific file (adjust the key as needed)
    validation_s3_key = f"{s3_upload_prefix}/<file_for_validation>.csv"
    validation_result = validate_csv_columns(
        s3_bucket=source_s3_bucket,
        s3_key=validation_s3_key,
        required_columns=required_columns
    )
    
    # 3. Branch the DAG based on the validation result
    branch = branch_validation(validation_result)
    
    # 4a. If validation passes, run the Glue job to process data
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name=glue_job_name,
        script_location=f"s3://{source_s3_bucket}/glue-scripts/<script_name>.py",
        s3_bucket=processed_data_s3_bucket,
        iam_role_name="<glue_iam_role>",
        create_job_kwargs={
            "GlueVersion": "2.0",
            "WorkerType": "Standard",
            "NumberOfWorkers": 2
        },
        on_success_callback=task_success_log
    )
    
    # 4b. If validation fails, an EmptyOperator ends the DAG.
    validation_failed = EmptyOperator(
        task_id="validation_failed",
        on_success_callback=task_success_log
    )
    
    # 5. Load processed data into DynamoDB
    load_data = load_to_dynamodb(
        processed_data_s3_bucket=processed_data_s3_bucket,
        processed_data_s3_key=processed_data_s3_key,
        dynamodb_table_name=dynamodb_table_name
    )
    
    # 6. Archive original data from S3
    archive_original = S3CopyObjectOperator(
        task_id="archive_original_data",
        source_bucket_name=source_s3_bucket,
        source_bucket_key=validation_s3_key,
        dest_bucket_name=archive_s3_bucket,
        dest_bucket_key=validation_s3_key,
        on_success_callback=task_success_log
    )
    
    # 7. Delete original data from the source S3 bucket
    delete_original = S3DeleteObjectsOperator(
        task_id="delete_original_data",
        bucket=source_s3_bucket,
        keys=validation_s3_key,
        on_success_callback=task_success_log
    )
    
    # Set task dependencies
    upload_files >> validation_result >> branch
    branch >> run_glue_job >> load_data >> archive_original >> delete_original
    branch >> validation_failed

s3_to_dynamodb_with_glue()
