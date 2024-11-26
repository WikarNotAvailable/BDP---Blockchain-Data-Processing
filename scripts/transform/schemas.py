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
        StructField("block_timestamp", TimestampType(), True),
        StructField("block_number", LongType(), True),
        StructField("transaction_hash", StringType(), True),
        StructField("transaction_index", LongType(), True),
        StructField("sender_address", StringType(), True),
        StructField("receiver_address", StringType(), True),
        StructField("transferred_value", DoubleType(), True),
        StructField("fee", DoubleType(), True),
    ]
)
