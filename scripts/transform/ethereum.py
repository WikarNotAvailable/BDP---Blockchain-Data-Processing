import os
import datetime
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("EthDataTransformation")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

s3 = boto3.client("s3", config=Config(signature_version=None))

current_date = datetime.datetime.now(datetime.timezone.utc)
seven_days_ago = current_date - datetime.timedelta(days=7)

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
transaction_files = []


def get_s3_objects(bucket, prefix):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )
    return response.get("Contents", [])


def download_last_7_parquets():
    objects = get_s3_objects(bucket_name, prefix)

    recent_files = [obj for obj in objects if obj["LastModified"] > seven_days_ago]

    recent_files.sort(key=lambda x: x["LastModified"], reverse=True)

    os.makedirs(data_dir, exist_ok=True)

    for obj in recent_files[:7]:  # Download only the top 7 files
        file_name = os.path.basename(obj["Key"])
        file_path = os.path.join(data_dir, file_name)

        print(f"Downloading {file_name}...")
        try:
            s3.download_file(bucket_name, obj["Key"], file_path)
            transaction_files.append(file_path)
            print(f"Downloaded {file_name} to {file_path}")
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")


# transaction_files = [
#     "eth-2024-11-22.snappy.parquet",
#     "eth-2024-11-21.snappy.parquet",
#     "eth-2024-11-20.snappy.parquet",
#     "eth-2024-11-19.snappy.parquet",
#     "eth-2024-11-18.snappy.parquet",
#     "eth-2024-11-17.snappy.parquet",
#     "eth-2024-11-16.snappy.parquet",
# ]
# transaction_files = [os.path.join(data_dir, file) for file in transaction_files]


def filter_and_transform(file_key):
    df = spark.read.parquet(file_key)

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

    # Calculate fee in ether
    df = df.withColumn("fee", (col("gas").cast("long") * col("gas_price")))

    # Drop unnecessary columns
    df = df.drop("gas", "gas_price")

    return df


all_data = None

download_last_7_parquets()

for file_key in transaction_files:
    print(f"Processing file: {file_key}")
    transformed_data = filter_and_transform(file_key)

    if all_data is None:
        all_data = transformed_data
    else:
        all_data = all_data.union(transformed_data)

# Save the transformed data to output file
if all_data.count() > 0:
    all_data = all_data.repartition(1)
    all_data.write.parquet(output_file, mode="overwrite", compression="snappy")
    [os.remove(file_path) for file_path in transaction_files]
    print(f"Transformed data saved to {output_file}")
else:
    print("No data found within the specified time range.")

# Stop the Spark session
spark.stop()
