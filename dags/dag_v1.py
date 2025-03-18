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
import boto3

ADMIN_EMAIL = "ghheskey@gmail.com"

def task_success_log(context):
    task_instance = context.get("task_instance")
    if task_instance:
        print(f"Task {task_instance.task_id} completed successfully.")

@task
def upload_files_to_s3(local_folder: str, s3_bucket: str, s3_prefix: str):
    s3 = S3Hook()
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_key = os.path.join(s3_prefix, relative_path)
            s3.load_file(filename=local_file_path, key=s3_key, bucket_name=s3_bucket, replace=True)
            print(f"Uploaded file {local_file_path} to s3://{s3_bucket}/{s3_key}")

@task
def process_ingested_data(ingestion_bucket: str, ingestion_prefix: str, processed_bucket: str, processed_prefix: str):
    s3 = S3Hook()

    # List and read all streaming data CSVs
    streaming_keys = s3.list_keys(bucket_name=ingestion_bucket, prefix=f"{ingestion_prefix}/streams/")
    streaming_dataframes = []
    for key in streaming_keys:
        if key.endswith(".csv"):
            content = s3.read_key(key, ingestion_bucket)
            df = pd.read_csv(io.StringIO(content))
            streaming_dataframes.append(df)
    all_streams = pd.concat(streaming_dataframes, ignore_index=True)

    # Read user metadata
    user_metadata_keys = s3.list_keys(bucket_name=ingestion_bucket, prefix=f"{ingestion_prefix}/users/")
    user_metadata_dfs = []
    for key in user_metadata_keys:
        if key.endswith(".csv"):
            content = s3.read_key(key, ingestion_bucket)
            df = pd.read_csv(io.StringIO(content))
            user_metadata_dfs.append(df)
    user_metadata = pd.concat(user_metadata_dfs, ignore_index=True)

    # Read song metadata
    song_metadata_keys = s3.list_keys(bucket_name=ingestion_bucket, prefix=f"{ingestion_prefix}/songs/")
    song_metadata_dfs = []
    for key in song_metadata_keys:
        if key.endswith(".csv"):
            content = s3.read_key(key, ingestion_bucket)
            df = pd.read_csv(io.StringIO(content))
            song_metadata_dfs.append(df)
    song_metadata = pd.concat(song_metadata_dfs, ignore_index=True)

    # Merge streaming data with user and song metadata
    merged = all_streams.merge(user_metadata, on='user_id', how='left').merge(song_metadata, on='track_id', how='left')

    # Save merged output to processed bucket
    output_buffer = io.StringIO()
    merged.to_csv(output_buffer, index=False)
    output_key = os.path.join(processed_prefix, f"processed_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
    s3.load_string(string_data=output_buffer.getvalue(), key=output_key, bucket_name=processed_bucket, replace=True)
    print(f"Processed data saved to s3://{processed_bucket}/{processed_prefix}/{output_key}")

@task
def start_glue_crawler(crawler_name: str):
    glue_client = boto3.client('glue')
    response = glue_client.start_crawler(Name=crawler_name)
    print(f"Glue crawler '{crawler_name}' started. Response: {response}")

@task
def validate_catalog_table(database_name: str, table_name: str, required_columns: list[str]):
    glue_client = boto3.client('glue')
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    columns = [col['Name'] for col in response['Table']['StorageDescriptor']['Columns']]
    missing_columns = [col for col in required_columns if col not in columns]
    if missing_columns:
        print(f"Validation failed. Missing columns: {missing_columns}")
        return False
    print("All required columns are present.")
    return True

@task.branch
def branch_on_validation(validation_passed: bool):
    return "run_glue_job" if validation_passed else "validation_failed"

@task
def load_processed_data_to_dynamodb(s3_bucket: str, prefix: str, dynamodb_table_name: str):
    s3 = S3Hook()
    dynamodb = DynamoDBHook()
    table = dynamodb.get_conn().Table(dynamodb_table_name)

    keys = s3.list_keys(bucket_name=s3_bucket, prefix=prefix)
    for key in keys:
        if key.endswith(".csv"):
            file_obj = s3.get_key(key=key, bucket_name=s3_bucket)
            content = file_obj.get()['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(content))
            with table.batch_writer() as batch:
                for _, row in df.iterrows():
                    batch.put_item(Item=row.to_dict())
            print(f"Loaded data from {key} into DynamoDB.")

@dag(
    dag_id="etl_with_processing_and_crawler",
    start_date=datetime(2025, 3, 17),
    schedule_interval="@hourly",
    catchup=False,
    default_args={"owner": "ec2_user", "email": [ADMIN_EMAIL], "email_on_failure": True, "retries": 2},
    tags=["AWS", "Glue", "S3", "DynamoDB"]
)
def etl_with_crawler_dag():
    local_folder = "../data/streams/"
    ingestion_bucket = "etl-ingestion-bucket-125"
    ingestion_prefix = "ingestion_folder/"
    processed_bucket = "processed-data-bucket-125"
    processed_prefix = "processed_folder/"
    archive_bucket = "archival-buckets-etl-125"
    glue_job_name = "music-etl"
    glue_crawler_name = "kpis-crawler"
    database_name = "kpi-crawler-db"
    table_name = "pre-processing-kpi"
    required_columns = ["user_id", "track_id", "listen_time"]
    dynamodb_table_name = "kpis-table"

    upload_task = upload_files_to_s3(local_folder=local_folder, s3_bucket=ingestion_bucket, s3_prefix=ingestion_prefix)
    process_task = process_ingested_data(
        ingestion_bucket=ingestion_bucket,
        ingestion_prefix=ingestion_prefix,
        processed_bucket=processed_bucket,
        processed_prefix=processed_prefix
    )
    start_crawler = start_glue_crawler(crawler_name=glue_crawler_name)
    catalog_validation = validate_catalog_table(database_name=database_name, table_name=table_name, required_columns=required_columns)
    branch_task = branch_on_validation(catalog_validation)

    run_glue = GlueJobOperator(
        task_id="run_glue_job",
        job_name=glue_job_name,
        script_location=f"s3://{processed_bucket}/glue-scripts/<script_name>.py",
        iam_role_name="<glue_iam_role>",
        create_job_kwargs={"GlueVersion": "2.0", "WorkerType": "Standard", "NumberOfWorkers": 2},
        on_success_callback=task_success_log
    )

    validation_failed = EmptyOperator(task_id="validation_failed")

    load_dynamodb = load_processed_data_to_dynamodb(
        s3_bucket=processed_bucket,
        prefix=processed_prefix,
        dynamodb_table_name=dynamodb_table_name
    )

    archive_processed = S3CopyObjectOperator(
        task_id="archive_processed_data",
        source_bucket_name=processed_bucket,
        source_bucket_key=processed_prefix,
        dest_bucket_name=archive_bucket,
        dest_bucket_key=processed_prefix,
        on_success_callback=task_success_log
    )

    delete_processed = S3DeleteObjectsOperator(
        task_id="delete_processed_data",
        bucket=processed_bucket,
        keys=processed_prefix,
        on_success_callback=task_success_log
    )

    # DAG flow
    upload_task >> process_task >> start_crawler >> catalog_validation >> branch_task
    branch_task >> run_glue >> load_dynamodb >> archive_processed >> delete_processed
    branch_task >> validation_failed

etl_with_crawler_dag()
