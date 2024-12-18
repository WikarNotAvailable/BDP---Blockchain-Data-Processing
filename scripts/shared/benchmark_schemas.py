from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    DoubleType,
    BooleanType
)
from scripts.shared.schemas import transaction_schema, aggregations_schema, aggregations_scaled_schema, transaction_scaled_schema,  joined_scaled_schema, ml_schema

benchmark_input_schema = StructType(
    [
        StructField("hash", StringType(), False),
        StructField("nonce", LongType(), False),
        StructField("transaction_index", LongType(), False),
        StructField("from_address", StringType(), False),
        StructField("to_address", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("gas", LongType(), False),
        StructField("gas_price", LongType(), False),
        StructField("input", StringType(), True),
        StructField("receipt_cumulative_gas_used", LongType(), False),
        StructField("receipt_gas_used", LongType(), False),
        StructField("block_timestamp", TimestampType(), False),  
        StructField("block_number", LongType(), False),
        StructField("block_hash", StringType(), False),
        StructField("from_scam", LongType(), True), 
        StructField("to_scam", LongType(), True),    
        StructField("from_category", StringType(), True),
        StructField("to_category", StringType(), True)
])

benchmark_transaction_schema = transaction_schema.add(StructField("label", BooleanType(), False))
benchmark_aggregations_schema = aggregations_schema
benchmark_aggregations_scaled_schema = aggregations_scaled_schema
benchmark_transaction_scaled_schema = transaction_scaled_schema.add(StructField("label", BooleanType(), False))
benchmark_joined_scaled_schema = joined_scaled_schema.add(StructField("label", BooleanType(), False))
benchmark_ml_schema = ml_schema.add(StructField("label", BooleanType(), False))
