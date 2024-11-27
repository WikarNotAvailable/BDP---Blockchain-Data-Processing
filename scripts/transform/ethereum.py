from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame
from schemas import eth_input_schema


def eth_transform(spark: SparkSession, file_name: str) -> DataFrame:

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
    ]

    df = spark.read.schema(eth_input_schema).parquet(file_name)

    df = (
        df.select(*fields_to_keep)
        .withColumnRenamed("hash", "transaction_hash")
        .withColumnRenamed("from_address", "sender_address")
        .withColumnRenamed("to_address", "receiver_address")
        .withColumnRenamed("value", "transferred_value")
        .withColumn("transferred_value", col("transferred_value").cast("double") / 10**18)
        .withColumn("gas_price", col("gas_price").cast("double") / 10**18)
        .withColumn("fee", col("gas").cast("double") * col("gas_price"))
        .drop("gas", "gas_price")
    )

    return df
