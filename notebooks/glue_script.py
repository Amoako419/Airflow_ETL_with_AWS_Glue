import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read streaming data from Glue Catalog
data = glueContext.create_data_frame.from_catalog(
    database="kpi-crawler-db",
    table_name="pre-processing-kpi"
)

# Extract date from timestamp
data = data.withColumn("created_date", F.to_date("created_at"))

# Calculate Listen Count, Unique Listeners, Total Listening Time, and Avg Listening Time per User
kpi_base = data.groupBy("created_date", "track_genre").agg(
    F.count("track_id").alias("listen_count"),
    F.countDistinct("user_id").alias("unique_listeners"),
    F.sum("duration_ms").alias("total_listening_time")
).withColumn(
    "avg_listening_time_per_user",
    F.col("total_listening_time") / F.when(F.col("unique_listeners") > 0, F.col("unique_listeners")).otherwise(1) / 1000
)

# Compute Top 3 Songs per Genre per Day
song_listen_count = data.groupBy(
    "created_date", "track_genre", "track_name"
).agg(
    F.count("track_id").alias("listen_count")
)

song_rank_window = Window.partitionBy("created_date", "track_genre").orderBy(F.desc("listen_count"))

top_songs_per_genre = song_listen_count.withColumn("rank", F.rank().over(song_rank_window)) \
                                        .filter(F.col("rank") <= 3) \
                                        .groupBy("created_date", "track_genre") \
                                        .agg(F.concat_ws(", ", F.collect_list("track_name")).alias("top_3"))

# Compute Top 5 Genres per Day
genre_listen_count = data.groupBy("created_date", "track_genre").agg(
    F.count("track_id").alias("listen_count")
)

genre_rank_window = Window.partitionBy("created_date").orderBy(F.desc("listen_count"))

top_genres_per_day = genre_listen_count.withColumn("rank", F.rank().over(genre_rank_window)) \
                                       .filter(F.col("rank") <= 5) \
                                       .groupBy("created_date") \
                                       .agg(F.concat_ws(", ", F.collect_list("track_genre")).alias("top_5_genres"))

# Join all KPIs
final_kpis = kpi_base.join(top_songs_per_genre, ["created_date", "track_genre"], "left") \
                     .join(top_genres_per_day, ["created_date"], "left")

# Write to S3 or DynamoDB (example: S3 in parquet)
glueContext.write_dynamic_frame.from_options(
    frame=glueContext.create_dynamic_frame.from_dataframe(final_kpis, glueContext),
    connection_type="s3",
    connection_options={"path": "s3://processed-data-bucket-125/processed_folder/"},
    format="csv"
)

job.commit()
