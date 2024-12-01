from pyspark.sql.functions import mean, mode, stddev, count, median, sum, min, max, col, lit
from pyspark.sql import SparkSession
from schemas import transaction_schema

def calculate_aggregations(df):
    sent_aggregations = (
        df.groupBy("sender_address")
        .agg(
            mean("sent_value").alias("avg_sent_value"),
            sum("sent_value").alias("total_sent_value"),
            min("sent_value").alias("min_sent_value"),
            max("sent_value").alias("max_sent_value"),
            median("sent_value").alias("median_sent_transactions"),
            mode("sent_value").alias("mode_sent_transactions"),
            stddev("sent_value").alias("stddev_sent_transactions"),
            count("sender_address").alias("num_sent_transactions"),
            # Sent exclusive
            mean("fee").alias("avg_fee_paid"),
            sum("fee").alias("total_fee_paid"),
            min("fee").alias("min_fee_paid"),
            max("fee").alias("max_fee_paid"),
        )
        .where(col("sender_address") != "NULL")
        .withColumnRenamed("sender_address", "address")
    )

    received_aggregations = (
        df.groupBy("receiver_address")
        .agg(
            mean("received_value").alias("avg_received_value"),
            sum("received_value").alias("total_received_value"),
            min("received_value").alias("min_received_value"),
            max("received_value").alias("max_received_value"),
            median("received_value").alias("median_received_transactions"),
            mode("received_value").alias("mode_received_transactions"),
            stddev("received_value").alias("stddev_received_transactions"),
            count("receiver_address").alias("num_received_transactions"),
        )
        .where(col("receiver_address") != "NULL")
        .withColumnRenamed("receiver_address", "address")
    )
    
    return sent_aggregations.join(received_aggregations, "address", "outer")

def create_aggregations_df(df):
    df_eth = df.where(col("network_name") == "ethereum")
    
    df_btc = df.where(col("network_name") == "bitcoin")
    df_btc_send = (
        df_btc.groupBy("sender_address", "transaction_hash")
        .agg(mean("sent_value").alias("sent_value"),
             mean("fee").alias("fee"))
        .withColumn("receiver_address", lit(None))
        .withColumn("received_value", lit(None))
    )
    df_btc_receive = (
        df_btc.groupBy("receiver_address", "transaction_hash")
        .agg(mean("received_value").alias("received_value"),
             mean("fee").alias("fee"))
        .withColumn("sender_address", lit(None))
        .withColumn("sent_value", lit(None))
    )
    df_btc = df_btc_send.unionByName(df_btc_receive)
    
    df_btc_aggregations = calculate_aggregations(df_btc)
    df_eth_aggregations = calculate_aggregations(df_eth)
    
    return df_btc_aggregations.unionByName(df_eth_aggregations).na.fill(0)

spark = (
    SparkSession.builder.appName("DataAggregations")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "4g")  # Increase executor memory
    .config("spark.driver.memory", "2g")    # Increase driver memory
    .config("spark.executor.cores", "4")    # Optionally, adjust executor cores
    .getOrCreate()
)

transaction_df = spark.read.schema(transaction_schema).parquet("results/transaction")

aggregations_df = create_aggregations_df(transaction_df)
output_dir = f"results/aggregations"  
aggregations_df.write.parquet(output_dir, mode="overwrite", compression="zstd")
spark.stop()