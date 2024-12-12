from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession, DataFrame
from scripts.shared.schemas import eth_schema


def eth_transform(spark: SparkSession, df: DataFrame) -> DataFrame:

    fields_to_keep = [
    "block_timestamp",
    "block_number",
    "hash",
    "transaction_index",
    "from_address",
    "to_address",
    "value",
    "gas_price",
    "gas",
    "from_scam",
    "to_scam"
    ]

    df = (
        df.select(*fields_to_keep)
        .withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("from_address", "sender_address")
        .withColumnRenamed("to_address", "receiver_address")
        .withColumnRenamed("value", "total_transferred_value") 
        .withColumn("network_name", lit("ethereum"))
        .withColumn("total_transferred_value", col("total_transferred_value").cast("double") / 10**18)
        .withColumn("gas_price", col("gas_price").cast("double") / 10**18)
        .withColumn("fee", col("gas").cast("double") * col("gas_price"))
        .withColumn("received_value", col("total_transferred_value"))
        .withColumn("sent_value", col("total_transferred_value") + col("fee"))
        .withColumn("total_input_value", col("total_transferred_value") + col("fee"))
        .withColumn("label", (col("from_scam") == lit(1)) | (col("to_scam") == lit(1)))
        .drop("gas", "gas_price", "from_scam", "to_scam")
    )
    return df
