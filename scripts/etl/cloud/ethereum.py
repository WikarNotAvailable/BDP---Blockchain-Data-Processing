import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, expr

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
def eth_transform(df: DataFrame) -> DataFrame:

    fields_to_keep = [
    "block_timestamp",
    "block_number",
    "hash",
    "transaction_index",
    "from_address",
    "to_address",
    "value",
    "gas_price",
    "gas"
    ]

    df_eth = df.filter(col("value") > 0)

    df_eth = (
        df_eth.select(*fields_to_keep)
        .withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("from_address", "sender_address")
        .withColumnRenamed("to_address", "receiver_address")
        .withColumnRenamed("value", "total_transferred_value") 
        .withColumn("network_name", lit("ethereum"))
        .withColumn("total_transferred_value", col("total_transferred_value").cast("double") / 10**18)
        .withColumn("gas_price", col("gas_price").cast("double") / 10**18)
        .withColumn("fee", col("gas").cast("double") * col("gas_price"))
        .withColumn("received_value", col("total_transferred_value"))
        .withColumn("sent_value", col("total_transferred_value") + col("fee"))
        .withColumn("total_input_value", col("total_transferred_value") + col("fee"))
        .withColumn("transaction_id", expr("uuid()"))
        .drop("gas", "gas_price")
    )

    return df_eth
    
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

spark = (
    SparkSession.builder.appName("DataETL")    
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


transactions = spark.read.parquet("s3://aws-public-blockchain/v1.0/eth/transactions/date=2024-12-11/")
result_df = eth_transform(transactions)

glueContext.write_data_frame.from_catalog(
    frame=result_df,
    database="bdp",
    table_name="cleaned_transactions"
)

job.commit()