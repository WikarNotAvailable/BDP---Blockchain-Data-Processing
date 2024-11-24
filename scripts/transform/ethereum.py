import os
import datetime
import time
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils import download_last_7_parquets
from schemas import eth_input_schema, transaction_output_schema

spark = (
    SparkSession.builder.appName("EthDataTransformation")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

s3 = boto3.client("s3", config=Config(signature_version=None))

current_date = datetime.datetime.now(datetime.timezone.utc)

bucket_name = "aws-public-blockchain"
prefix = f"v1.0/eth/transactions/date={current_date.year}"

home_dir = os.path.expanduser("~")
data_dir = os.path.join(home_dir, "Desktop", "aws-data", "eth")

output_file = "eth-data"
fields_to_keep = [
    "block_timestamp",
    "block_number",
    "hash",
    "transaction_index",
    "from_address",
    "to_address",
    "value",
    "gas_price",
    "gas",
]


def filter_and_transform(file_key):
    df = spark.read.schema(eth_input_schema).parquet(file_key)

    # Filter fields
    df = df.select(*fields_to_keep)

    # Rename columns
    df = (
        df.withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("from_address", "sender_address")
        .withColumnRenamed("to_address", "receiver_address")
        .withColumnRenamed("value", "transferred_value")
    )

    # Convert gas_price and transferred_value from wei to ether
    df = df.withColumn(
        "gas_price", col("gas_price").cast("double") / 10**18
    ).withColumn(
        "transferred_value", col("transferred_value").cast("double") / 10**18
    )

    # Convert block_timestamp from timestamp to string
    df = df.withColumn("block_timestamp", col("block_timestamp").cast("string"))

    # Calculate fee in ether
    df = df.withColumn("fee", (col("gas").cast("long") * col("gas_price")))

    # Drop unnecessary columns
    df = df.drop("gas", "gas_price")

    return df


all_data = None

transaction_files = download_last_7_parquets(s3, bucket_name, prefix, data_dir)

# Transform data
for file_key in transaction_files:
    print(f"Processing file: {file_key}")
    transformed_data = filter_and_transform(file_key)

    if all_data is None:
        all_data = spark.createDataFrame(
            transformed_data.rdd, transaction_output_schema
        )
    else:
        all_data = all_data.union(transformed_data)

# Save the transformed data to output file
if all_data.count() > 0:
    all_data.coalesce(1).write.parquet(
        output_file, mode="overwrite", compression="snappy"
    )

    [os.remove(file_path) for file_path in transaction_files]

    print(f"Transformed data saved to {output_file}")
else:
    print("No data found within the specified time range.")

# Stop the Spark session
spark.stop()
