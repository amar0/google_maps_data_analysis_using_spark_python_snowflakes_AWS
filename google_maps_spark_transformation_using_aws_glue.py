import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import *
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
s3_path = "s3://google-maps-etl-project-amar/raw_data/to_process/"

source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": [s3_path]},
    format_options={"withHeader": True},
    transformation_ctx="source_dyf",
)

google_maps_df = source_dyf.toDF()

test_df = google_maps_df
test_df.withColumn("data", explode("data")).show(5, False)

def process_google_maps_data(df):
    df = df.withColumn("data", explode("data"))
    df = df.select(
        col("data.name").alias("name"),
        #col("data.full_address").alias("address"),
        regexp_replace(col("data.full_address"), ",", "").alias("address"),
        col("data.review_count").alias("review_count"),
        col("data.phone_number").alias("phone_number"),
        #col("data.rating").alias("rating"),
        col("data.place_link").alias("place_link")
    )
    df = df.drop_duplicates(["name"])
    return df

test_df.withColumn("data", explode("data")).select(
    col("data.name").alias("name"),
    col("data.full_address").alias("address"),
    col("data.review_count").alias("review_count"),
    col("data.phone_number").alias("phone_number"),
    col("data.rating").alias("rating"),
    col("data.place_link").alias("place_link")).drop_duplicates(["name"]).show(5)

google_maps_processed_df = process_google_maps_data(test_df)

def write_to_s3(df, s3_path, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"s3://google-maps-etl-project-amar/transformed_data/{s3_path}/"},
        format=format_type
    )

write_to_s3(google_maps_processed_df, "maps_data/transformed_data_{}".format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S")), "csv")
job.commit()