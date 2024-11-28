from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    DoubleType,
    BooleanType,
    ArrayType
)

eth_schema = StructType(
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

btc_inputs_column_schema = ArrayType(StructType(
    [
        StructField("index", LongType(), False),
        StructField("spent_transaction_hash", StringType(), True),
        StructField("spent_output_index", LongType(), True),
        StructField("script_asm", StringType(), True),
        StructField("script_hex", StringType(), True),
        StructField("sequence", LongType(), True),
        StructField("required_signatures", LongType(), True),
        StructField("type", StringType(), True),
        StructField("address", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("txinwitness", ArrayType(StringType()), True),
    ]
))

btc_outputs_column_schema = ArrayType(StructType(
    [
        StructField("index", LongType(), False),
        StructField("script_asm", StringType(), True),
        StructField("script_hex", StringType(), True),
        StructField("required_signatures", LongType(), True),
        StructField("type", StringType(), True),
        StructField("address", StringType(), True),
        StructField("value", DoubleType(), True),
    ]
))

btc_schema = StructType(
    [
        StructField("index", LongType(), False),
        StructField("hash", StringType(), False),
        StructField("size", LongType(), True),
        StructField("virtual_size", LongType(), True),
        StructField("version", LongType(), True),
        StructField("lock_time", LongType(), True),
        StructField("block_hash", StringType(), False),
        StructField("block_number", LongType(), False),
        StructField("block_timestamp", TimestampType(), False),
        StructField("input_count", LongType(), True),
        StructField("output_count", LongType(), True),
        StructField("input_value", DoubleType(), True),
        StructField("output_value", DoubleType(), True),
        StructField("is_coinbase", BooleanType(), True),
        StructField("fee", DoubleType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("date", StringType(), True),
        StructField("inputs", btc_inputs_column_schema, True),
        StructField("outputs", btc_outputs_column_schema, True),
    ]
)

crypto_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("block_timestamp", TimestampType(), False),
        StructField("block_number", LongType(), False),
        StructField("transaction_hash", StringType(), False),
        StructField("transaction_index", LongType(), False),
        StructField("fee", DoubleType(), True),
        StructField("sender_address", StringType(), True),
        StructField("receiver_address", StringType(), True),
        StructField("total_transferred_value", DoubleType(), True),
        StructField("sent_value", DoubleType(), True),
        StructField("received_value", DoubleType(), True),
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
