import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, expr, explode
from datetime import datetime, timedelta
import boto3
import re


def validate_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_params(start_date: str, end_date: str):
    print(f"START_DATE: {start_date}, END_DATE: {end_date}")
    if not start_date or not end_date:
        print("Error: START_DATE and END_DATE parameters are required.")
        sys.exit(1)

    if not validate_date(start_date):
        print(f"Error: START_DATE '{start_date}' is not in the correct format (YYYY-MM-DD).")
        sys.exit(1)

    if not validate_date(end_date):
        print(f"Error: END_DATE '{end_date}' is not in the correct format (YYYY-MM-DD).")
        sys.exit(1)

    if start_date > end_date:
        print("Error: START_DATE cannot be later than END_DATE.")
        sys.exit(1)


def btc_transform(df: DataFrame) -> DataFrame:

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
    

    df_btc = (
        df.select(*fields_to_keep)
        .withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("index", "transaction_index")
        .withColumnRenamed("input_value", "total_input_value")
        .withColumn("total_transferred_value", col("total_input_value") - col("fee"))
        .withColumn("network_name", lit("bitcoin"))
        .withColumn("sender_address", col("input.address"))
        .withColumn("receiver_address", col("output.address"))
        .withColumn("sent_value", col("input.value"))
        .withColumn("received_value", col("output.value"))
        .withColumn("transaction_id", expr("uuid()"))
        .filter(col("total_transferred_value") > 0)
        .drop("output", 'input')
    )

    return df_btc


def get_s3_objects(s3, bucket: str, prefix: str, start_date: str) -> list:
    """
    List S3 objects starting from a given date prefix with pagination support.

    :param s3: Boto3 S3 client.
    :param bucket: S3 bucket name.
    :param prefix: Prefix path in the bucket.
    :param start_date: Start date in 'YYYY-MM-DD' format.
    :return: List of S3 objects.
    """
    objects = []
    continuation_token = None 

    while True:
        params = {
            'Bucket': bucket,
            'Prefix': prefix,
            'StartAfter': f"{prefix}date={start_date}"
        }
        if continuation_token:
            params['ContinuationToken'] = continuation_token

        try:
            response = s3.list_objects_v2(**params)
            contents = response.get("Contents", [])
            objects.extend(contents)
        
            if not response.get('IsTruncated'):
                break  

            continuation_token = response.get('NextContinuationToken')

        except Exception as e:
            print(f"Error listing S3 objects: {e}")
            break

    return objects


def filter_files_by_date(bucket: str, file_names: list, start_date: str, end_date: str) -> list[str]:
    """
    Filter S3 objects based on a date range.
    
    :param bucket: S3 bucket name.
    :param file_names: List of S3 objects.
    :param start_date: Start date in 'YYYY-MM-DD' format.
    :param end_date: End date in 'YYYY-MM-DD' format.
    :return: List of filtered S3 object keys.
    """
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    regex = r'date=(\d{4}-\d{2}-\d{2})'

    filtered_file_names = []
    for file_name in file_names:
        match = re.search(regex, file_name['Key'])
        if match:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
            if start_datetime <= file_date <= end_datetime:
                filtered_file_names.append(f"s3://{bucket}/{file_name['Key']}")

    return filtered_file_names


def setup_blockchain_db(spark):
    spark.sql("""
    CREATE DATABASE IF NOT EXISTS bdp
    """)


def setup_iceberg_table(spark):
    spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.bdp.cleaned_transactions (
        transaction_id STRING,
        block_timestamp TIMESTAMP,
        block_number BIGINT,
        transaction_hash STRING,
        transaction_index BIGINT,
        fee DOUBLE,
        sender_address STRING,
        receiver_address STRING,
        total_transferred_value DOUBLE,
        total_input_value DOUBLE,
        sent_value DOUBLE,
        received_value DOUBLE,
        network_name STRING
    )
    PARTITIONED BY (network_name, date(block_timestamp))
    LOCATION 's3://bdp-cleaned-transactions'
    TBLPROPERTIES ('table_type' = 'ICEBERG', 'write.format.default'='parquet', 'write.parquet.compression-codec'='zstd')
    """)


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'START_DATE', 'END_DATE'])
start_date = args['START_DATE']
end_date = args['END_DATE']
validate_params(start_date, end_date)

spark = (
    SparkSession.builder.appName("BitcoinETL")    
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.mergeSchema", "true") # No need as we explicitly specify the schema
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bdp-cleaned-transactions/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.adaptive.enabled", "true") # Keep partitions in simmilar size
    .getOrCreate()
)

setup_blockchain_db(spark)
setup_iceberg_table(spark)

glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
bucket_name = 'aws-public-blockchain'
prefix = 'v1.0/btc/transactions/'

all_files = get_s3_objects(s3, bucket_name, prefix, start_date)
print(f"Found {len(all_files)} files in the S3 bucket.")
filtered_files = filter_files_by_date(bucket_name, all_files, start_date, end_date)
print(f"Filtered {len(filtered_files)} files for the specified date range.")

if not filtered_files:
    print("No files found for the specified date range.")
    job.commit()
    sys.exit(0)

transactions = spark.read.parquet(*filtered_files)
result_df = btc_transform(transactions)

glueContext.write_data_frame.from_catalog(
    frame=result_df,
    database="bdp",
    table_name="cleaned_transactions"
)

job.commit()
spark.stop()
