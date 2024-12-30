from pyspark.sql import SparkSession
from scripts.local.shared.schemas import joined_scaled_schema, transaction_schema, aggregations_schema
from pyspark.sql.functions import col
from scripts.local.anomalies_detection.preprocessing.components.preprocess_data_first_step import first_preprocess_training_data
from scripts.local.anomalies_detection.preprocessing.components.preprocess_data_second_step import second_preprocess_training_data
from scripts.local.anomalies_detection.preprocessing.components.preprocess_data_third_step import third_preprocess_training_data

spark = (
    SparkSession.builder.appName("DataPreprocessing")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "6g")  # Increase executor memory
    .config("spark.driver.memory", "2g")    # Increase driver memory
    #.config("spark.local.dir", "/mnt/d/spark-temp")  # Change temp directory
    .getOrCreate()
)

transactions_df = spark.read.schema(transaction_schema).parquet("data/historical/etl/transactions")
transactions_ethereum_df = transactions_df.where(col("network_name")=="ethereum")
transactions_bitcoin_df = transactions_df.where(col("network_name")=="bitcoin")

aggregations_df = spark.read.schema(aggregations_schema).parquet("data/historical/aggregations")
aggregations_ethereum_df = aggregations_df.where(col("network_name")=="ethereum")
aggregations_bitcoin_df = aggregations_df.where(col("network_name")=="bitcoin")

#Preprocessing divied into two steps due to memory caching problems, launch one step at the time
step = 3
if step == 1:
    first_preprocess_training_data(transactions_ethereum_df, transactions_bitcoin_df, aggregations_ethereum_df, aggregations_bitcoin_df)
elif step == 2:
    second_preprocess_training_data(spark)
else: 
    wallets_aggregations_merged_df = spark.read.schema(joined_scaled_schema).parquet("data/historical/preprocessing/merged_networks_transactions_with_aggregations")

    wallets_aggregations_eth_df = spark.read.schema(joined_scaled_schema).parquet("data/historical/preprocessing/eth_with_aggregations")
    
    wallets_aggregations_btc_df = spark.read.schema(joined_scaled_schema).parquet("data/historical/preprocessing/btc_with_aggregations")
    
    third_preprocess_training_data(wallets_aggregations_merged_df, wallets_aggregations_eth_df, wallets_aggregations_btc_df)