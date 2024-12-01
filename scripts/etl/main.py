import boto3
import datetime
from botocore.client import Config
from botocore import UNSIGNED
from pyspark.sql import SparkSession
import tempfile
from bitcoin import btc_transform
from ethereum import eth_transform
from etl import extract, load, transform
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

def perform_etl(spark, s3, bucket_name, prefix, transform_func, days, temp_dir, label):
    transactions = extract(s3, bucket_name, prefix, days, temp_dir)
    df = transform(spark, transactions, transform_func)
    load(df, label)
    return df

def generate_uuid():
    return str(uuid.uuid4())

spark = (
    SparkSession.builder.appName("DataETL")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "4g")  # Increase executor memory
    .config("spark.driver.memory", "2g")    # Increase driver memory
    .config("spark.executor.cores", "4")    # Optionally, adjust executor cores
    .getOrCreate()
)

uuid_udf = udf(generate_uuid, StringType())

current_date = datetime.datetime.now(datetime.timezone.utc)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
bucket_name = "aws-public-blockchain"
eth_prefix = f"v1.0/eth/transactions/date={current_date.year}"
btc_prefix = f"v1.0/btc/transactions/date={current_date.year}"

try:
    with tempfile.TemporaryDirectory() as temp_dir:
        eth_df = perform_etl(spark, s3, bucket_name, eth_prefix, eth_transform, 7, temp_dir, 'eth')
        btc_df = perform_etl(spark, s3, bucket_name, btc_prefix, btc_transform, 7, temp_dir, 'btc')

        transaction_df = btc_df.unionByName(eth_df).withColumn("transaction_id", uuid_udf())
        load(transaction_df, 'transaction') # Here we recalcualte both eth and btc df due to 'write'. We could use cache()/persist() to avoid it but it causes memory issues. We can also read eth and btc from parquets

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    spark.stop()
