from pyspark.sql import SparkSession
from schemas import btc_input_schema
from pyspark.sql.functions import col, lit, explode


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
    
    df = spark.read.schema(btc_input_schema).parquet(file_name)

    # Filter fields
    df = df.select(*fields_to_keep)

    # Rename columns
    df = (
        df.withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("index", "transaction_index")
        .withColumnRenamed("input_value", "total_transferred_value")
    )
    
    # Transformations
    df = (
        df.withColumn("network_name", lit("bitcoin"))
        .withColumn("sender_address", col("input.address"))
        .withColumn("receiver_address", col("output.address"))
        .withColumn("sent_value", col("input.value"))
        .withColumn("received_value", col("output.value"))
    )
    
    # Columns drop
    df = (
        df.drop("input")
        .drop("output")
    )

    return df
