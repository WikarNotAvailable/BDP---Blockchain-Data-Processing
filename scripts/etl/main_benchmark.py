from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from benchmark_components.etl import extract, transform, load
from benchmark_components.ethereum import eth_transform


def perform_etl(spark, csv_path, transform_func, label):
    transactions = extract(spark, csv_path)
    df = transform(spark, transactions, transform_func)
    df = df.withColumn("transaction_id", expr("uuid()"))
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

csv_eth_path = "data/benchmark/source"
eth_df = perform_etl(spark, csv_eth_path, eth_transform, "transactions")

spark.stop()
