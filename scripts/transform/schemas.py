from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    DoubleType,
)

eth_input_schema = StructType(
    [
        StructField("gas", LongType(), True),
        StructField("hash", StringType(), True),
        StructField("input", StringType(), True),
        StructField("nonce", LongType(), True),
        StructField("value", DoubleType(), True),
        StructField("block_number", LongType(), True),
        StructField("block_hash", StringType(), True),
        StructField("transaction_index", LongType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("gas_price", LongType(), True),
        StructField("receipt_cumulative_gas_used", LongType(), True),
        StructField("receipt_gas_used", LongType(), True),
        StructField("receipt_contract_address", StringType(), True),
        StructField("receipt_status", LongType(), True),
        StructField("receipt_effective_gas_price", LongType(), True),
        StructField("transaction_type", LongType(), True),
        StructField("max_fee_per_gas", LongType(), True),
        StructField("max_priority_fee_per_gas", LongType(), True),
        StructField("block_timestamp", TimestampType(), True),
        StructField("date", StringType(), True),
        StructField("last_modified", TimestampType(), True),
    ]
)

transaction_output_schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("block_timestamp", TimestampType(), True),
        StructField("block_number", LongType(), True),
        StructField("transaction_hash", StringType(), True),
        StructField("transaction_index", LongType(), True),
        StructField("sender_address", StringType(), True),
        StructField("receiver_address", StringType(), True),
        StructField("total_transferred_value", DoubleType(), True),
        StructField("sent_value", DoubleType(), True),
        StructField("received_value", DoubleType(), True),
        StructField("fee", DoubleType(), True),
        StructField("network_name", StringType(), True),
    ]
)

aggregations_schema = StructType(
    [
        StructField("avg_sent_value", DoubleType(), True),
        StructField("avg_received_value", DoubleType(), True),
        StructField("total_sent_value", DoubleType(), True),
        StructField("total_received_value", DoubleType(), True),
        StructField("min_sent_value", DoubleType(), True),
        StructField("min_received_value", DoubleType(), True),
        StructField("max_sent_value", DoubleType(), True),
        StructField("max_received_value", DoubleType(), True),
        StructField("median_sent_value", DoubleType(), True),
        StructField("median_received_value", DoubleType(), True),
        StructField("mode_sent_value", DoubleType(), True),
        StructField("mode_received_value", DoubleType(), True),
        StructField("stddev_sent_value", DoubleType(), True),
        StructField("stddev_received_value", DoubleType(), True),
        StructField("num_sent_transactions", LongType(), True),
        StructField("num_received_transactions", LongType(), True),
        StructField("avg_fee_paid", DoubleType(), True),
        StructField("total_fee_paid", DoubleType(), True),
        StructField("min_fee_paid", DoubleType(), True),
        StructField("max_fee_paid", DoubleType(), True),
    ]
)
