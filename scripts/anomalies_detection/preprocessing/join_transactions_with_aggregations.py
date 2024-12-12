from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from scripts.shared.schemas import transaction_schema, aggregations_schema

spark = (
    SparkSession.builder.appName("DataAggregations")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "6g")  # Increase executor memory
    .config("spark.driver.memory", "2g")    # Increase driver memory
    #.config("spark.local.dir", "/mnt/d/spark-temp")  # Change temp directory
    .getOrCreate()
)

sender_fields = [
    "avg_received_value",
    "avg_total_value_for_receiver",
    "sum_received_value",
    "sum_total_value_for_receiver",
    "min_received_value",
    "min_total_value_for_receiver",
    "max_received_value",
    "max_total_value_for_receiver",
    "median_received_value",
    "median_total_value_for_receiver",
    "mode_received_value",
    "mode_total_value_for_receiver",
    "stddev_received_value",
    "stddev_total_value_for_receiver",
    "num_received_transactions",
    "avg_time_between_received_transactions",
    "avg_incoming_speed_count",
    "avg_incoming_speed_value",
    "avg_incoming_acceleration_count",
    "avg_incoming_acceleration_value",
    "unique_in_degree",
    "avg_fee_paid",
    "total_fee_paid",
    "min_fee_paid",
    "max_fee_paid",
]

receiver_fields = [
    "avg_sent_value",
    "avg_total_value_for_sender",
    "sum_sent_value",
    "sum_total_value_for_sender",
    "min_sent_value",
    "min_total_value_for_sender",
    "max_sent_value",
    "max_total_value_for_sender",
    "median_sent_value",
    "median_total_value_for_sender",
    "mode_sent_value",
    "mode_total_value_for_sender",
    "stddev_sent_value",
    "stddev_total_value_for_sender",
    "num_sent_transactions",
    "avg_time_between_sent_transactions",
    "avg_outgoing_speed_count",
    "avg_outgoing_speed_value",
    "avg_outgoing_acceleration_count",
    "avg_outgoing_acceleration_value",
    "unique_out_degree",
]

common_fields = [
    "address",
    "activity_duration",
    "first_transaction_timestamp",
    "last_transaction_timestamp"
]
transactions_df = spark.read.schema(transaction_schema).parquet("results/transaction")
aggregations_df = spark.read.schema(aggregations_schema).parquet("results/aggregations")

sender_aggregations = aggregations_df.select(
    sender_fields + [col(field).alias(f"{field}_for_sender") for field in common_fields]
)
receiver_aggregations = aggregations_df.select(
    receiver_fields + [col(field).alias(f"{field}_for_receiver") for field in common_fields]
)

transactions_with_sender = transactions_df.join(
    sender_aggregations,
    transactions_df["sender_address"] == sender_aggregations["address_for_sender"],
    "left"
).drop("address_for_sender")
final_df = transactions_with_sender.join(
    receiver_aggregations,
    transactions_with_sender["receiver_address"] == receiver_aggregations["address_for_receiver"],
    "left"
).drop("address_for_receiver")

final_df.coalesce(1).write.parquet(
    "results/joined_transactions_with_aggregations", mode="overwrite", compression="zstd"
)

spark.stop()