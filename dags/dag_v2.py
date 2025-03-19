from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.operators.python import ShortCircuitOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import io
import os
import boto3
from dotenv import load_dotenv
import logging

# Configure logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv("ACCESS_KEYS"),
    aws_secret_access_key=os.getenv("SECRET_KEYS"),
    region_name=os.getenv("REGION")
)

def task_success_log(context):
    task_instance = context.get("task_instance")
    if task_instance:
        logger.info(f"Task {task_instance.task_id} completed successfully.")

@task
def check_if_files_exist(bucket_name: str, prefix: str, aws_conn_id: str):
    logger.info(f"Checking for files in bucket '{bucket_name}' with prefix '{prefix}'...")
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix)
    if keys:
        msg = f"Found {len(keys)} files with prefix '{prefix}' in bucket '{bucket_name}'"
        logger.info(msg)
        return {"result": True, "message": msg}
    else:
        msg = f"No files found with prefix '{prefix}' in bucket '{bucket_name}'"
        logger.info(msg)
        return {"result": False, "message": msg}

@task
def process_ingested_data(ingestion_bucket: str, ingestion_prefix: str, processed_bucket: str, processed_prefix: str):
    logger.info("Starting ingestion data processing...")
    s3 = S3Hook(aws_conn_id="aws_conn_id")

    # Read streaming data
    streaming_keys = s3.list_keys(bucket_name=ingestion_bucket, prefix=f"{ingestion_prefix}streams/")
    logger.info(f"Streaming keys found: {streaming_keys}")
    streaming_dataframes = []
    for key in streaming_keys:
        if key.endswith(".csv"):
            content = s3.read_key(key, ingestion_bucket)
            df = pd.read_csv(io.StringIO(content))
            streaming_dataframes.append(df)
    logger.info(f"Read {len(streaming_dataframes)} streaming data files.")
    all_streams = pd.concat(streaming_dataframes, ignore_index=True)

    # Read user metadata
    user_metadata_keys = s3.list_keys(bucket_name=ingestion_bucket, prefix=f"{ingestion_prefix}users/")
    logger.info(f"User metadata keys found: {user_metadata_keys}")
    user_metadata_dfs = []
    for key in user_metadata_keys:
        if key.endswith(".csv"):
            content = s3.read_key(key, ingestion_bucket)
            df = pd.read_csv(io.StringIO(content))
            user_metadata_dfs.append(df)
    logger.info(f"Read {len(user_metadata_dfs)} user metadata files.")
    user_metadata = pd.concat(user_metadata_dfs, ignore_index=True)

    # Read song metadata
    song_metadata_keys = s3.list_keys(bucket_name=ingestion_bucket, prefix=f"{ingestion_prefix}songs/")
    logger.info(f"Song metadata keys found: {song_metadata_keys}")
    song_metadata_dfs = []
    for key in song_metadata_keys:
        if key.endswith(".csv"):
            content = s3.read_key(key, ingestion_bucket)
            df = pd.read_csv(io.StringIO(content))
            song_metadata_dfs.append(df)
    logger.info(f"Read {len(song_metadata_dfs)} song metadata files.")
    song_metadata = pd.concat(song_metadata_dfs, ignore_index=True)

    # Merge data
    logger.info("Merging stream data with user and song metadata...")
    merged = all_streams.merge(user_metadata, on='user_id', how='left').merge(song_metadata, on='track_id', how='left')

    # Save merged data
    output_buffer = io.StringIO()
    merged.to_csv(output_buffer, index=False)
    output_key = os.path.join(processed_prefix, f"processed_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
    s3.load_string(string_data=output_buffer.getvalue(), key=output_key, bucket_name=processed_bucket, replace=True)
    logger.info(f"Processed data saved to s3://{processed_bucket}/{output_key}")

@task
def start_glue_crawler(crawler_name: str):
    logger.info(f"Starting Glue crawler: {crawler_name}")
    glue_client = session.client('glue')
    response = glue_client.start_crawler(Name=crawler_name)
    logger.info(f"Glue crawler '{crawler_name}' started. Response: {response}")

@task
def validate_catalog_table(database_name: str, table_name: str, required_columns: list[str]):
    logger.info(f"Validating Glue catalog table: {database_name}.{table_name}")
    glue_client = session.client('glue')
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    columns = [col['Name'] for col in response['Table']['StorageDescriptor']['Columns']]
    missing_columns = [col for col in required_columns if col not in columns]
    if missing_columns:
        message = f"Validation failed. Missing columns: {missing_columns}"
        logger.warning(message)
        return {"result": False, "message": message}
    message = "All required columns are present."
    logger.info(message)
    return {"result": True, "message": message}

def branch_on_validation(ti):
    validation_data = ti.xcom_pull(task_ids="validate_catalog_table")
    result = validation_data.get("result")
    message = validation_data.get("message")
    logger.info(f"Branch decision based on validation: {message}")
    if result:
        return "run_glue_job"
    else:
        return "validation_failed"

@task.branch
def decide_next_step(ti):
    file_check_data = ti.xcom_pull(task_ids="check_if_files_exist")
    if file_check_data["result"]:
        logger.info("Files found. Proceeding to process data.")
        return "process_ingested_data"
    else:
        logger.info("No files found. Ending DAG.")
        return "end_dag"

@task
def archive_processed_files(source_bucket, source_prefix, dest_bucket, dest_prefix):
    logger.info(f"Archiving processed files from {source_bucket}/{source_prefix} to {dest_bucket}/{dest_prefix}")
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    keys = s3.list_keys(bucket_name=source_bucket, prefix=source_prefix)
    for key in keys:
        dest_key = key.replace(source_prefix, dest_prefix, 1)
        s3.copy_object(
            source_bucket_key=key,
            source_bucket_name=source_bucket,
            dest_bucket_name=dest_bucket,
            dest_bucket_key=dest_key
        )
        logger.info(f"Archived {key} to {dest_bucket}/{dest_key}")

@dag(
    dag_id="etl_with_glue_spark",
    start_date=datetime(2025, 3, 17),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "ec2_user", "retries": 2},
    tags=["AWS", "Glue", "S3", "DynamoDB"]
)
def etl_with_crawler_dag():
    ingestion_bucket = "etl-ingestion-bucket-125"
    ingestion_prefix = "ingestion_folder/"
    processed_bucket = "processed-data-bucket-125"
    processed_prefix = "processed_folder/"
    archive_bucket = "archival-buckets-etl-125"
    glue_job_name = "music-etl"
    glue_crawler_name = "kpis-crawler"
    database_name = "kpi-crawler-db"
    table_name = "processed_folder"
    archive_prefix = "archived-folder/"
    required_columns = [
        "user_id", "track_id", "listen_time",
        "track_genre", "created_at", "duration_ms",
        "track_name", "user_name", "artists",
        "popularity", "user_age", "user_country", "album_name", "explicit"
    ]

    check_files_task = check_if_files_exist(
        bucket_name=ingestion_bucket,
        prefix=ingestion_prefix,
        aws_conn_id="aws_conn_id"
    )

    branch_decision = decide_next_step()
    process_task = process_ingested_data(
        ingestion_bucket=ingestion_bucket,
        ingestion_prefix=ingestion_prefix,
        processed_bucket=processed_bucket,
        processed_prefix=processed_prefix
    )
    end_dag = EmptyOperator(task_id="end_dag")

    start_crawler = start_glue_crawler(crawler_name=glue_crawler_name)
    catalog_validation = validate_catalog_table(
        database_name=database_name,
        table_name=table_name,
        required_columns=required_columns
    )
    branch_task = BranchPythonOperator(
        task_id="branch_on_validation",
        python_callable=branch_on_validation,
        provide_context=True,
    )

    run_glue = GlueJobOperator(
        task_id="run_glue_job",
        job_name=glue_job_name,
        on_success_callback=task_success_log,
        wait_for_completion=True,
        script_args={
            "--JOB_NAME": glue_job_name,
            "--DYNAMODB_TABLE_NAME": "kpi-table_v2",
            "--AWS_REGION": "eu-west-1"
        },
        verbose=True,
        aws_conn_id="aws_conn_id"
    )

    validation_failed = EmptyOperator(task_id="validation_failed")

    archive_processed = archive_processed_files(
        processed_bucket, processed_prefix, archive_bucket, archive_prefix
    )

    delete_processed = S3DeleteObjectsOperator(
        task_id="delete_processed_data",
        bucket="processed-data-bucket-125",
        prefix="processed_folder/processed_data_",
        aws_conn_id="aws_conn_id",
        on_success_callback=task_success_log
    )

    check_files_task >> branch_decision
    branch_decision >> process_task
    branch_decision >> end_dag

    process_task >> start_crawler >> catalog_validation >> branch_task
    branch_task >> run_glue >> archive_processed >> delete_processed
    branch_task >> validation_failed

etl_with_crawler_dag()
