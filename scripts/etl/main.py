import boto3
import datetime
from botocore.client import Config
from botocore import UNSIGNED
from pyspark.sql import SparkSession
import tempfile
from components.bitcoin import btc_transform
from components.ethereum import eth_transform
from etl import extract, load, transform
from pyspark.sql.functions import expr


def perform_etl(spark, s3, bucket_name, prefix, transform_func, start_date, end_date, temp_dir, label):
    transactions = extract(s3, bucket_name, prefix, start_date, end_date, temp_dir)
    df = transform(spark, transactions, transform_func)
    load(df, label)
    return df

spark = (
    SparkSession.builder.appName("DataETL")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "6g")
    .config("spark.driver.memory", "2g")
    #.config("spark.local.dir", "/mnt/d/spark-temp")  # Change local dir to avoid permission issues
    .getOrCreate()
)

current_date = datetime.datetime.now(datetime.timezone.utc)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

bucket_name = "aws-public-blockchain"
eth_prefix = "v1.0/eth/transactions/"
btc_prefix = "v1.0/btc/transactions/"
start_date = "2024-11-25"
end_date = "2024-12-01"

try:
    with tempfile.TemporaryDirectory() as temp_dir:
        eth_df = perform_etl(spark, s3, bucket_name, eth_prefix, eth_transform, start_date, end_date, temp_dir, 'eth')
        btc_df = perform_etl(spark, s3, bucket_name, btc_prefix, btc_transform, start_date, end_date, temp_dir, 'btc')

        transaction_df = btc_df.unionByName(eth_df).withColumn("transaction_id", expr("uuid()"))
        load(transaction_df, 'transaction') # Here we recalcualte both eth and btc df due to 'write'. We could use cache()/persist() to avoid it but it causes memory issues. We can also read eth and btc from parquets

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    spark.stop()
