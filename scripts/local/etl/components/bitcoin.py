from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode
from scripts.local.shared.schemas import btc_schema


def btc_transform(spark: SparkSession, file_name: str):

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
    
    df = spark.read.schema(btc_schema).parquet(file_name)

    df = (
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
        .filter(col("total_transferred_value") > 0)
        .drop("output", 'input')
    )

    return df
