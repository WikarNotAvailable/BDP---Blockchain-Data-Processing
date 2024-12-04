from pyspark.sql.functions import mean, mode, stddev, count, median, sum, min, max, col, lit, count_distinct, unix_timestamp, lag, first, when
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from schemas import transaction_schema

def calculate_aggregations(df):
    sender_window = Window.partitionBy("sender_address").orderBy("block_timestamp")
    receiver_window = Window.partitionBy("receiver_address").orderBy("block_timestamp")

    df = (
        df.withColumn("sender_time_diff", unix_timestamp("block_timestamp") - unix_timestamp(lag("block_timestamp").over(sender_window)))
        .withColumn("receiver_time_diff", unix_timestamp("block_timestamp") - unix_timestamp(lag("block_timestamp").over(receiver_window)))  
    )

    active_duration_df = calculate_active_duration(df)

    sent_aggregations = (
        df.groupBy("sender_address")
        .agg(
            mean("sent_value").alias("avg_sent_value"),
            sum("sent_value").alias("sum_sent_value"),
            min("sent_value").alias("min_sent_value"),
            max("sent_value").alias("max_sent_value"),
            median("sent_value").alias("median_sent_transactions"),
            mode("sent_value").alias("mode_sent_transactions"),
            stddev("sent_value").alias("stddev_sent_transactions"),
            
            mean("total_transferred_value").alias("avg_total_value_for_sender"),
            sum("total_transferred_value").alias("sum_total_value_for_sender"),
            min("total_transferred_value").alias("min_total_value_for_sender"),
            max("total_transferred_value").alias("max_total_value_for_sender"),
            median("total_transferred_value").alias("median_total_value_for_sender"),
            mode("total_transferred_value").alias("mode_total_value_for_sender"),
            stddev("total_transferred_value").alias("stddev_total_value_for_sender"),

            count("sender_address").alias("num_sent_transactions"),
            count_distinct("receiver_address").alias("num_sent_transactions_to_unique"),
            mean("sender_time_diff").alias("avg_time_between_sent_transactions"),

            sum("sender_time_diff").alias("total_outgoing_time"),
            # Sent exclusive
            mean("fee").alias("avg_fee_paid"),
            sum("fee").alias("total_fee_paid"),
            min("fee").alias("min_fee_paid"),
            max("fee").alias("max_fee_paid"),
        )
        .where(col("sender_address") != "NULL")
        .withColumn("avg_outgoing_speed_count", when(col("total_outgoing_time") > 0, col("num_sent_transactions") / col("total_outgoing_time")).otherwise(0))
        .withColumn("avg_outgoing_speed_value", when(col("total_outgoing_time") > 0, col("sum_sent_value") / col("total_outgoing_time")).otherwise(0))
        .withColumn("avg_outgoing_acceleration_count", when(col("total_outgoing_time") > 0, col("avg_outgoing_speed_count") / col("total_outgoing_time")).otherwise(0))
        .withColumn("avg_outgoing_acceleration_value", when(col("total_outgoing_time") > 0, col("avg_outgoing_speed_value") / col("total_outgoing_time")).otherwise(0))
        .drop("total_outgoing_time")
        .withColumnRenamed("sender_address", "address")
    )

    received_aggregations = (
        df.groupBy("receiver_address")
        .agg(
            mean("received_value").alias("avg_received_value"),
            sum("received_value").alias("sum_received_value"),
            min("received_value").alias("min_received_value"),
            max("received_value").alias("max_received_value"),
            median("received_value").alias("median_received_transactions"),
            mode("received_value").alias("mode_received_transactions"),
            stddev("received_value").alias("stddev_received_transactions"),

            mean("received_value").alias("avg_total_value_for_receiver"),
            sum("received_value").alias("sum_total_value_for_receiver"),
            min("received_value").alias("min_total_value_for_receiver"),
            max("received_value").alias("max_total_value_for_receiver"),
            median("received_value").alias("median_total_value_for_receiver"),
            mode("received_value").alias("mode_total_value_for_receiver"),
            stddev("received_value").alias("stddev_total_value_for_receiver"),

            count("receiver_address").alias("num_received_transactions"),
            count_distinct("sender_address").alias("num_received_transactions_from_unique"),
            mean("receiver_time_diff").alias("avg_time_between_received_transactions"),

            sum("receiver_time_diff").alias("total_incoming_time")
        )
        .where(col("receiver_address") != "NULL")
        .withColumn("avg_incoming_speed_count", when(col("total_incoming_time") > 0, col("num_received_transactions") /col("total_incoming_time")).otherwise(0))
        .withColumn("avg_incoming_speed_value", when(col("total_incoming_time") > 0, col("sum_received_value") /col("total_incoming_time")).otherwise(0))
        .withColumn("avg_incoming_acceleration_count", when(col("total_incoming_time") > 0, col("avg_incoming_speed_count") / col("total_incoming_time")).otherwise(0))
        .withColumn("avg_incoming_acceleration_value", when(col("total_incoming_time") > 0, col("avg_incoming_speed_value") / col("total_incoming_time")).otherwise(0))
        .drop("total_incoming_time")
        .withColumnRenamed("receiver_address", "address")
    )
    
    return sent_aggregations.join(received_aggregations, "address", "outer").join(active_duration_df, "address", "left")

def calculate_active_duration(df) -> DataFrame:
    min_max_timestamps = (
        df.select(
            col("sender_address").alias("address"), 
            col("block_timestamp").alias("timestamp")
        )
        .union(
            df.select(
                col("receiver_address").alias("address"), 
                col("block_timestamp").alias("timestamp")
            )
        )
        .groupBy("address")
        .agg(
            min("timestamp").alias("first_transaction_timestamp"),
            max("timestamp").alias("last_transaction_timestamp")
        )
    )

    return min_max_timestamps.withColumn("activity_duration", unix_timestamp("last_transaction_timestamp") - unix_timestamp("first_transaction_timestamp"))


def calculate_unique_degrees(df):
    out_degrees = (
        df.groupBy("sender_address")
        .agg(count_distinct("receiver_address").alias("unique_out_degree"))
        .withColumnRenamed("sender_address", "address")
        )
    
    in_degrees = (
        df.groupBy("receiver_address")
        .agg(count_distinct("sender_address").alias("unique_in_degree"))
        .withColumnRenamed("receiver_address", "address")
    )

    return out_degrees.join(in_degrees, "address", "outer").na.fill(0)

def preprocess_btc_df(df):

    df_btc_send = (
        df_btc.groupBy("sender_address", "transaction_hash")
        .agg(mean("sent_value").alias("sent_value"),
             mean("fee").alias("fee"),
             first("total_transferred_value").alias("total_transferred_value"),
             first("block_timestamp").alias("block_timestamp"))
            .withColumn("receiver_address", lit(None))
            .withColumn("received_value", lit(None))
    )

    df_btc_receive = (
        df_btc.groupBy("receiver_address", "transaction_hash")
        .agg(mean("received_value").alias("received_value"),
             mean("fee").alias("fee"),
             first("total_transferred_value").alias("total_transferred_value"),
             first("block_timestamp").alias("block_timestamp"))
            .withColumn("sender_address", lit(None))
            .withColumn("sent_value", lit(None))
    )

    return df_btc_send.unionByName(df_btc_receive)
    
    
spark = (
    SparkSession.builder.appName("DataAggregations")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "8g")  # Increase executor memory
    .config("spark.driver.memory", "8g")    # Increase driver memory
    .config("spark.executor.cores", "4")    # Optionally, adjust executor cores
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12")
    .getOrCreate()
)

transaction_df = spark.read.schema(transaction_schema).parquet("results/transaction")
unique_degrees_df = calculate_unique_degrees(transaction_df)

df_eth = transaction_df.where(col("network_name") == "ethereum")
df_btc = transaction_df.where(col("network_name") == "bitcoin")

df_btc = preprocess_btc_df(transaction_df)

df_btc_aggregations = calculate_aggregations(df_btc)
df_eth_aggregations = calculate_aggregations(df_eth)

aggregations_df = df_btc_aggregations.unionByName(df_eth_aggregations)
aggregations_df = aggregations_df.join(unique_degrees_df, "address", "outer").na.fill(0)

output_dir = f"results/aggregations"  
aggregations_df.coalesce(1).write.parquet(output_dir, mode="overwrite", compression="zstd")


spark.stop()

