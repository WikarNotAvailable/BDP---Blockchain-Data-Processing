from pyspark.sql.functions import col
from scripts.local.shared.consts import sender_fields, receiver_fields, common_fields

def join_transactions_with_aggregations(spark, transactions_dir, aggregations_dir, output_dir, transactions_scaled_schema, aggregations_scaled_schema):
    transactions_df = spark.read.schema(transactions_scaled_schema).parquet(transactions_dir)
    aggregations_df = spark.read.schema(aggregations_scaled_schema).parquet(aggregations_dir)

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
        output_dir, 
        mode="overwrite", 
        compression="zstd"
    )
    
    return final_df