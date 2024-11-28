from pyspark.sql.functions import mean, mode, stddev, count, median
from pyspark.sql import SparkSession
from schemas import crypto_schema

def calculate_aggregations(df):
    sent_aggregations = (
        df.groupBy("sender_address")
        .agg(
            mean("total_transferred_value").alias("avg_sent_value"),
            sum("total_transferred_value").alias("total_sent_value"),
            min("total_transferred_value").alias("min_sent_value"),
            max("total_transferred_value").alias("max_sent_value"),
            median("total_transferred_value").alias("median_sent_transactions"),
            mode("total_transferred_value").alias("mode_sent_transactions"),
            stddev("total_transferred_value").alias("stddev_sent_transactions"),
            count("sender_address").alias("num_sent_transactions"),
            # Sent exclusive
            mean("fee").alias("avg_fee_paid"),
            sum("fee").alias("total_fee_paid"),
            min("fee").alias("min_fee_paid"),
            max("fee").alias("max_fee_paid"),
        )
        .withColumnRenamed("sender_address", "address")
    )

    received_aggregations = (
        df.groupBy("receiver_address")
        .agg(
            mean("total_transferred_value").alias("avg_received_value"),
            sum("total_transferred_value").alias("total_received_value"),
            min("total_transferred_value").alias("min_received_value"),
            max("total_transferred_value").alias("max_received_value"),
            median("total_transferred_value").alias("median_received_transactions"),
            mode("total_transferred_value").alias("mode_received_transactions"),
            stddev("total_transferred_value").alias("stddev_received_transactions"),
            count("receiver_address").alias("num_received_transactions"),
        )
        .withColumnRenamed("receiver_address", "address")
    )

    return sent_aggregations.join(received_aggregations, "address", "outer")

spark = (
    SparkSession.builder.appName("EthDataTransformation")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .getOrCreate()
)

crypto_df = spark.read.schema(crypto_schema).parquet("results/crypto")

spark.stop()
