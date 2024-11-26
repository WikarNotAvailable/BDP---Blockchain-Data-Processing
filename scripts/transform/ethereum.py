import datetime
import boto3
import shutil
from botocore.client import Config
from botocore import UNSIGNED
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    mean,
    sum,
    count,
    max,
    min,
    median,
    mode,
    stddev,
)
from utils import download_last_7_parquets
from schemas import eth_input_schema
from functools import reduce
import time


spark = (
    SparkSession.builder.appName("EthDataTransformation")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config(
        "spark.sql.parquet.mergeSchema", "false"
    )  # No need as we explicitly specify the schema
    .getOrCreate()
)

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

current_date = datetime.datetime.now(datetime.timezone.utc)

bucket_name = "aws-public-blockchain"
prefix = f"v1.0/eth/transactions/date={current_date.year}"

data_dir = "data"
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

    df = df.select(*fields_to_keep)

    # Add sent_value, received_value and network_name columns
    df = (
        df.withColumn("sent_value", col("total_transferred_value"))
        .withColumn("received_value", col("total_transferred_value"))
        .withColumn("network_name", lit("ethereum"))
    )

    # Rename columns
    df = (
        df.withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("from_address", "sender_address")
        .withColumnRenamed("to_address", "receiver_address")
        .withColumnRenamed("value", "total_transferred_value")
    )

    # Convert values from wei to eth
    df = df.withColumn(
        "total_transferred_value",
        col("total_transferred_value").cast("double") / 10**18,
    ).withColumn("gas_price", col("gas_price").cast("double") / 10**18)

    # Calculate fee column
    df = df.withColumn("fee", col("gas").cast("double") * col("gas_price"))

    # Drop unnecessary column
    df = df.drop("gas", "gas_price")

    return df


def calculate_aggregations(df):
    sent_aggregations = (
        df.groupBy("sender_address")
        .agg(
            mean("total_transferred_value").alias("avg_sent_value"),
            sum("total_transferred_value").alias("total_sent_value"),
            min("total_transferred_value").alias("min_sent_value"),
            max("total_transferred_value").alias("max_sent_value"),
            median("total_transferred_value").alias("median_sent_transactions"),
            mode("total_transferred_value").alias("mode_sent_transactions"),
            stddev("total_transferred_value").alias("stddev_sent_transactions"),
            count("sender_address").alias("num_sent_transactions"),
            # Sent exclusive
            mean("fee").alias("avg_fee_paid"),
            sum("fee").alias("total_fee_paid"),
            min("fee").alias("min_fee_paid"),
            max("fee").alias("max_fee_paid"),
        )
        .withColumnRenamed("sender_address", "address")
    )

    received_aggregations = (
        df.groupBy("receiver_address")
        .agg(
            mean("total_transferred_value").alias("avg_received_value"),
            sum("total_transferred_value").alias("total_received_value"),
            min("total_transferred_value").alias("min_received_value"),
            max("total_transferred_value").alias("max_received_value"),
            median("total_transferred_value").alias("median_received_transactions"),
            mode("total_transferred_value").alias("mode_received_transactions"),
            stddev("total_transferred_value").alias("stddev_received_transactions"),
            count("receiver_address").alias("num_received_transactions"),
        )
        .withColumnRenamed("receiver_address", "address")
    )

    return sent_aggregations.join(received_aggregations, "address", "outer")


transaction_files = download_last_7_parquets(s3, bucket_name, prefix, data_dir)

# Transform data
start_time = time.time()
all_data_list = []
all_data = None

for file_key in transaction_files:
    print(f"Processing file: {file_key}")
    transformed_data = filter_and_transform(file_key)
    all_data_list.append(transformed_data)

if all_data_list:
    all_data = reduce(
        DataFrame.union, all_data_list
    )  # For large datasets it may be better to do incremental union in a loop

# Calculate aggregations for addresses
aggregated_df = calculate_aggregations(all_data)

if not all_data.isEmpty():
    all_data.coalesce(1).write.parquet(
        f"{output_file}/data",
        mode="overwrite",
        compression="zstd",
    )

    aggregated_df.coalesce(1).write.parquet(
        f"{output_file}/aggregations",
        mode="overwrite",
        compression="zstd",
    )

    shutil.rmtree(data_dir)

    print(f"Transformed data saved to {output_file}")
else:
    print("No data found within the specified time range.")

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")

spark.stop()
