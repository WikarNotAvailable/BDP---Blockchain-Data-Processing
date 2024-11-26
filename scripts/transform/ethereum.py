import datetime
import boto3
import shutil
from botocore.client import Config
from botocore import UNSIGNED
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from utils import download_last_7_parquets
from schemas import eth_input_schema
from functools import reduce
import time


spark = (
    SparkSession.builder.appName("EthDataTransformation")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
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

    df = (
        df.select(*fields_to_keep)
        .withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("from_address", "sender_address")
        .withColumnRenamed("to_address", "receiver_address")
        .withColumnRenamed("value", "transferred_value")
        .withColumn("transferred_value", col("transferred_value").cast("double") / 10**18)
        .withColumn("gas_price", col("gas_price").cast("double") / 10**18)
        .withColumn("fee", col("gas").cast("double") * col("gas_price"))
        .drop("gas", "gas_price")
    )

    return df


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
    all_data = reduce(DataFrame.union, all_data_list) # For large datasets it may be better to do incremental union in a loop

if not all_data.isEmpty():
    all_data.coalesce(1).write.parquet(output_file, mode="overwrite", compression="zstd")

    shutil.rmtree(data_dir)

    print(f"Transformed data saved to {output_file}")
else:
    print("No data found within the specified time range.")

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")

spark.stop()
