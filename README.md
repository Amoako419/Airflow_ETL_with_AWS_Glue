Overview
========

# Data Pipeline with Airflow & AWS GLUE

This project is a **real-time ETL pipeline** designed to process streaming music listening data, compute daily key performance indicators (KPIs) such as top songs and genres, and store results in **Amazon DynamoDB** for fast lookups. It ensures scalability, automation, and traceability, enabling real-time analytics for music streaming platforms.

### Purpose
This project was designed to address the challenges of processing large volumes of streaming data efficiently and reliably. By leveraging cloud-native tools, the pipeline ensures scalability, fault tolerance, and automation. It provides actionable insights such as top genres, top songs, and user engagement metrics, which are critical for decision-making in the music streaming industry.

Architecture Diagram
======
<p align="center">
    <img src="images/architecture_diagram.jpg" alt="The architecture diagram" width="100%" />
</p>

Technologies Used
==================
| Service            | Why it was used                                                                                        |
|--------------------|-------------------------------------------------------------------------------------------------------|
| **Amazon S3**      | Acts as the data lake for ingesting streaming data, user profiles, and song metadata.                |
| **AWS Glue**       | Used for data transformation, cleaning, KPI computation, and writing the results directly to DynamoDB.|
| **Glue Crawlers**  | Automatically discover schema and keep AWS Glue Data Catalog up-to-date.                             |
| **Apache Airflow** | Orchestrates pipeline tasks, triggers the Glue job on schedule, and handles notifications.            |
| **Amazon DynamoDB**| Chosen as the destination to store daily KPIs for fast and scalable lookups.                         |
| **Boto3 (Python SDK)** | Used inside the Glue script to write transformed KPI data directly into DynamoDB.              |

Architecture & Data Flow
===
1. Data Source:
    - Streaming data is ingested as batch files in Amazon S3 at irregular intervals.
2. Processing:
    - Validation: Ensure files have the required schema before processing.
    - Transformation: Clean and normalize data using AWS Glue (PySpark & Python Shell jobs).
    - Metric Computation: Compute key KPIs for real-time insights.
3. Storage & Consumption:
    - Processed data is stored in Amazon DynamoDB for fast lookups by downstream applications.

<p align="center">
    <img src="images/etl_with_glue -.png" alt="The architecture diagram" width="100%" />
</p>

How the Pipeline Works 
============= 

1. **Data Ingestion:**  
   - Streaming listening data, user profiles, and song metadata are uploaded into respective folders in S3.  
2. **Crawler Execution:**  
   - Glue Crawler scans the S3 processed folder to update the Glue Catalog with fresh schema.  
3. **Airflow DAG Trigger:**  
   - Airflow schedules the job every day.  
   - If validation succeeds, it runs the Glue job.  
4. **Glue ETL Job:**  
   - Reads processed data from the Glue Catalog  
   - Performs transformations and KPI computations  
   - Converts float values into `Decimal` (required by DynamoDB)  
   - Populates results directly into DynamoDB.  
5. **Post-processing:**  
   - Processed data is archived to an archival S3 bucket.  
   - Original processed folder contents are deleted, leaving the structure intact.  

Key Features
============
 1. Real-Time Processing: Handles data as it arrives without batch scheduling.
 2. Scalable & Cloud-Native: Leverages AWS Glue, S3, DynamoDB, and Airflow for orchestration.
 3. Automated Workflow: Orchestrated with Apache Airflow, ensuring smooth execution and monitoring.
 4. Failure Handling: Sends email alerts if the pipeline fails.
 5. **Enhanced Observability**: Added logging to all major tasks in the pipeline to track execution progress and debug issues effectively.

DynamoDB Table Schema  
=============

| Attribute                     | Type    | Description                                         |
|--------------------------------|---------|-----------------------------------------------------|
| `date`                         | String  | The date for which KPIs are computed (partition key)|
| `track_genre`                  | String  | The genre of the tracks (sort key)                  |
| `listen_count`                 | Number  | Total listen count                                  |
| `unique_listeners`             | Number  | Unique listeners count                              |
| `total_listening_time`         | Number  | Total listening time in milliseconds                |
| `avg_listening_time_per_user`  | Decimal | Average listening time per user (stored as Decimal) |
| `top_3_songs`                  | String  | Comma-separated list of top 3 songs                 |
| `top_5_genres`                 | String  | Comma-separated list of top 5 genres of the day     |


KPIs Computed
=============
| KPI                           | Description                                                                    |
|--------------------------------|--------------------------------------------------------------------------------|
| `listen_count`                 | Number of listens per genre per day.                                           |
| `unique_listeners`             | Number of unique listeners per genre per day.                                  |
| `total_listening_time`         | Sum of all listening durations (in milliseconds) per genre per day.            |
| `avg_listening_time_per_user`  | Average listening time (in seconds) per user, per genre per day.               |
| `top_3_songs`                  | Top 3 most listened-to songs per genre per day (comma-separated).              |
| `top_5_genres`                 | Top 5 most popular genres per day (comma-separated).                           |




Cleanup and Archiving Process  
==============

- After the Glue job finishes successfully:  
  - All processed data is moved from the `processed` folder in S3 to the `archive` bucket.  
  - Only the folder structure remains; all file contents are deleted.  


Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
    - `dag_v1`: This DAG orchestrates the ETL pipeline for processing streaming music listening data. It includes tasks for data ingestion, validation, transformation, KPI computation, and loading results into Amazon DynamoDB. The DAG is designed for scalability and fault tolerance, ensuring smooth execution of the pipeline. **Logging has been added to all tasks for better observability.**
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- notebooks: 
    - `eda.ipynb`: This Jupyter Notebook contains exploratory data analysis (EDA) for the streaming music dataset. It includes  statistical summaries to understand the data distribution, identify anomalies, and validate the schema before processing. The notebook serves as a reference for designing the data transformation logic.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Future Improvements 
======== 

- Add unit tests and automated validation for processed KPIs.  
- Add API Gateway + Lambda for real-time KPI retrieval.  
- Add support for hourly KPIs in addition to daily metrics.  

