import datetime
import boto3
import shutil
from botocore.client import Config
from pyspark.sql import SparkSession, DataFrame
from utils import download_last_7_parquets
from schemas import btc_input_schema
from functools import reduce
from botocore import UNSIGNED
from pyspark.sql.functions import col, lit, explode

spark = (
    SparkSession.builder.appName("BtcDataTransformation")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .getOrCreate()
)

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

current_date = datetime.datetime.now(datetime.timezone.utc)

bucket_name = "aws-public-blockchain"
prefix = f"v1.0/btc/transactions/date={current_date.year}"

data_dir = "data"
output_file = "btc-data"
fields_to_keep = [
    "block_timestamp",
    "block_number",
    "fee",
    "hash",
    "index",
    "input_value", 
    explode("inputs").alias("input"), 
    explode("outputs").alias("output"),  
]


def filter_and_transform(file_key):
    df = spark.read.schema(btc_input_schema).parquet(file_key)

    # Filter fields
    df = df.select(*fields_to_keep)

    # Rename columns
    df = (
        df.withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("index", "transaction_index")
        .withColumnRenamed("input_value", "total_transferred_value")
    )
    
    # Transformations
    df = (
        df.withColumn("network_name", lit("bitcoin"))
        .withColumn("sender_address", col("input.address"))
        .withColumn("receiver_address", col("output.address"))
        .withColumn("sent_value", col("input.value"))
        .withColumn("received_value", col("output.value"))
    )
    
    # Columns drop
    df = (
        df.drop("input")
        .drop("output")
    )

    return df

transaction_files = download_last_7_parquets(s3, bucket_name, prefix, data_dir)
    
# Transform data
all_data_list = []
all_data = None

for file_key in transaction_files:
    print(f"Processing file: {file_key}")
    transformed_data = filter_and_transform(file_key)
    all_data_list.append(transformed_data)

if all_data_list:
    all_data = reduce(DataFrame.union, all_data_list) # For large datasets it may be better to do incremental union in a loop

if not all_data.isEmpty():
    all_data.coalesce(1).write.parquet(output_file, mode="overwrite", compression="zstd")

    shutil.rmtree(data_dir)

    print(f"Transformed data saved to {output_file}")
else:
    print("No data found within the specified time range.")

spark.stop()