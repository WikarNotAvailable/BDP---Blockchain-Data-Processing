import boto3
import datetime
import shutil
from botocore.client import Config
from botocore import UNSIGNED
from pyspark.sql import SparkSession
from bitcoin import btc_transform
from ethereum import eth_transform
from etl import extract, load, transform


current_date = datetime.datetime.now(datetime.timezone.utc)

spark = (
    SparkSession.builder.appName("EthDataTransformation")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .getOrCreate()
)

bucket_name = "aws-public-blockchain"
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

temp_dir = "temp_data"

eth_prefix = f"v1.0/eth/transactions/date={current_date.year}"
eth_transactions = extract(s3, bucket_name, eth_prefix)
eth_df = transform(spark, eth_transactions, eth_transform)
load(eth_df, 'eth')

btc_prefix = f"v1.0/btc/transactions/date={current_date.year}"
btc_transactions = extract(s3, bucket_name, btc_prefix)
btc_df = transform(spark, btc_transactions, btc_transform)
load(btc_df, 'btc')

shutil.rmtree(temp_dir)  # remove temporary directory with downloaded files

spark.stop()
