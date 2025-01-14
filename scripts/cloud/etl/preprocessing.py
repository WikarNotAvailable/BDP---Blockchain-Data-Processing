from pyspark.sql.functions import col
from pyspark.sql.functions import udf, col, when
from pyspark.ml.feature import VectorAssembler, RobustScaler
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql.functions import unix_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def setup_blockchain_db(spark):
    spark.sql("""
    CREATE DATABASE IF NOT EXISTS bdp
    """)


def setup_iceberg_table(spark):
    spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.bdp.scaled_features (
        block_timestamp FLOAT,
        block_number FLOAT,
        transaction_index FLOAT,
        fee FLOAT,
        total_transferred_value FLOAT,
        total_input_value FLOAT,
        sent_value FLOAT,
        received_value FLOAT,
        network_name BOOLEAN,
        avg_sent_value FLOAT,
        avg_received_value FLOAT,
        avg_total_value_for_sender FLOAT,
        avg_total_value_for_receiver FLOAT,
        sum_sent_value FLOAT,
        sum_received_value FLOAT,
        sum_total_value_for_sender FLOAT,
        sum_total_value_for_receiver FLOAT,
        min_sent_value FLOAT,
        min_received_value FLOAT,
        min_total_value_for_sender FLOAT,
        min_total_value_for_receiver FLOAT,
        max_sent_value FLOAT,
        max_received_value FLOAT,
        max_total_value_for_sender FLOAT,
        max_total_value_for_receiver FLOAT,
        median_sent_value FLOAT,
        median_received_value FLOAT,
        median_total_value_for_sender FLOAT,
        median_total_value_for_receiver FLOAT,
        mode_sent_value FLOAT,
        mode_received_value FLOAT,
        mode_total_value_for_sender FLOAT,
        mode_total_value_for_receiver FLOAT,
        stddev_sent_value FLOAT,
        stddev_received_value FLOAT,
        stddev_total_value_for_sender FLOAT,
        stddev_total_value_for_receiver FLOAT,
        num_sent_transactions FLOAT,
        num_received_transactions FLOAT,
        avg_time_between_sent_transactions FLOAT,
        avg_time_between_received_transactions FLOAT,
        avg_outgoing_speed_count FLOAT,
        avg_incoming_speed_count FLOAT,
        avg_outgoing_speed_value FLOAT,
        avg_incoming_speed_value FLOAT,
        avg_outgoing_acceleration_count FLOAT,
        avg_incoming_acceleration_count FLOAT,
        avg_outgoing_acceleration_value FLOAT,
        avg_incoming_acceleration_value FLOAT,
        avg_fee_paid FLOAT,
        total_fee_paid FLOAT,
        min_fee_paid FLOAT,
        max_fee_paid FLOAT,
        activity_duration_for_sender FLOAT,
        first_transaction_timestamp_for_sender FLOAT,
        last_transaction_timestamp_for_sender FLOAT,
        activity_duration_for_receiver FLOAT,
        first_transaction_timestamp_for_receiver FLOAT,
        last_transaction_timestamp_for_receiver FLOAT,
        unique_out_degree FLOAT,
        unique_in_degree FLOAT
    )                                                                      
    PARTITIONED BY (network_name)
    LOCATION 's3://bdp-scaled-features'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """)
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.bdp.unscaled_features (
        block_timestamp BIGINT,
        block_number BIGINT,
        transaction_index BIGINT,
        fee FLOAT,
        total_transferred_value FLOAT,
        total_input_value FLOAT,
        sent_value FLOAT,
        received_value FLOAT,
        network_name BOOLEAN,
        avg_sent_value FLOAT,
        avg_received_value FLOAT,
        avg_total_value_for_sender FLOAT,
        avg_total_value_for_receiver FLOAT,
        sum_sent_value FLOAT,
        sum_received_value FLOAT,
        sum_total_value_for_sender FLOAT,
        sum_total_value_for_receiver FLOAT,
        min_sent_value FLOAT,
        min_received_value FLOAT,
        min_total_value_for_sender FLOAT,
        min_total_value_for_receiver FLOAT,
        max_sent_value FLOAT,
        max_received_value FLOAT,
        max_total_value_for_sender FLOAT,
        max_total_value_for_receiver FLOAT,
        median_sent_value FLOAT,
        median_received_value FLOAT,
        median_total_value_for_sender FLOAT,
        median_total_value_for_receiver FLOAT,
        mode_sent_value FLOAT,
        mode_received_value FLOAT,
        mode_total_value_for_sender FLOAT,
        mode_total_value_for_receiver FLOAT,
        stddev_sent_value FLOAT,
        stddev_received_value FLOAT,
        stddev_total_value_for_sender FLOAT,
        stddev_total_value_for_receiver FLOAT,
        num_sent_transactions BIGINT,
        num_received_transactions BIGINT,
        avg_time_between_sent_transactions FLOAT,
        avg_time_between_received_transactions FLOAT,
        avg_outgoing_speed_count FLOAT,
        avg_incoming_speed_count FLOAT,
        avg_outgoing_speed_value FLOAT,
        avg_incoming_speed_value FLOAT,
        avg_outgoing_acceleration_count FLOAT,
        avg_incoming_acceleration_count FLOAT,
        avg_outgoing_acceleration_value FLOAT,
        avg_incoming_acceleration_value FLOAT,
        avg_fee_paid FLOAT,
        total_fee_paid FLOAT,
        min_fee_paid FLOAT,
        max_fee_paid FLOAT,
        activity_duration_for_sender BIGINT,
        first_transaction_timestamp_for_sender BIGINT,
        last_transaction_timestamp_for_sender BIGINT,
        activity_duration_for_receiver BIGINT,
        first_transaction_timestamp_for_receiver BIGINT,
        last_transaction_timestamp_for_receiver BIGINT,
        unique_out_degree BIGINT,
        unique_in_degree BIGINT
    )                                                                      
    PARTITIONED BY (network_name)
    LOCATION 's3://bdp-unscaled-features'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """)


cols_dict = {
    "transactions_numeric" : [
        "block_number", 
        "block_timestamp",
        "transaction_index", 
        "fee", 
        "total_transferred_value", 
        "total_input_value", 
        "sent_value", 
        "received_value"
    ],

    "aggregations_numeric" : [
        "avg_sent_value",
        "avg_received_value",
        "avg_total_value_for_sender",
        "avg_total_value_for_receiver",
        "sum_sent_value",
        "sum_received_value",
        "sum_total_value_for_sender",
        "sum_total_value_for_receiver",
        "min_sent_value",
        "min_received_value",
        "min_total_value_for_sender",
        "min_total_value_for_receiver",
        "max_sent_value",
        "max_received_value",
        "max_total_value_for_sender",
        "max_total_value_for_receiver",
        "median_sent_value",
        "median_received_value",
        "median_total_value_for_sender",
        "median_total_value_for_receiver",
        "mode_sent_value",
        "mode_received_value",
        "mode_total_value_for_sender",
        "mode_total_value_for_receiver",
        "stddev_sent_value",
        "stddev_received_value",
        "stddev_total_value_for_sender",
        "stddev_total_value_for_receiver",
        "num_sent_transactions",
        "num_received_transactions",
        "avg_time_between_sent_transactions",
        "avg_time_between_received_transactions",
        "avg_outgoing_speed_count",
        "avg_incoming_speed_count",
        "avg_outgoing_speed_value",
        "avg_incoming_speed_value",
        "avg_outgoing_acceleration_count",
        "avg_incoming_acceleration_count",
        "avg_outgoing_acceleration_value",
        "avg_incoming_acceleration_value",
        "avg_fee_paid",
        "total_fee_paid",
        "min_fee_paid",
        "max_fee_paid",
        "unique_out_degree",
        "unique_in_degree",
        "activity_duration_for_sender",
        "activity_duration_for_receiver",
        "first_transaction_timestamp_for_sender",
        "first_transaction_timestamp_for_receiver",
        "last_transaction_timestamp_for_sender",
        "last_transaction_timestamp_for_receiver"
    ],

    "transactions_datetime" : [
        "block_timestamp"
    ],

    "aggregations_datetime" : [
        "first_transaction_timestamp",
        "last_transaction_timestamp"
    ],

    "transactions_string" : [
        "transaction_id",
        "transaction_hash",
        "sender_address",
        "receiver_address",
        "network_name"
    ],

    "receiver_fields" : [
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
    ],

    "sender_fields" : [
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
    ],

    "common_fields" : [
        "address",
        "activity_duration",
        "first_transaction_timestamp",
        "last_transaction_timestamp"
    ]
}


def join_transactions_with_aggregations(transactions_df, aggregations_df, cols_dict):

    sender_aggregations = aggregations_df.select(
        cols_dict["sender_fields"] + [col(field).alias(f"{field}_for_sender") for field in cols_dict["common_fields"]]
    )
    receiver_aggregations = aggregations_df.select(
        cols_dict["receiver_fields"] + [col(field).alias(f"{field}_for_receiver") for field in cols_dict["common_fields"]]
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

    
    return final_df

def scale_numeric_variables(numeric_cols, df):
    assembler_vector = VectorAssembler(inputCols=numeric_cols, outputCol="features_to_scale")
    df_assembled = assembler_vector.transform(df)

    scaler = RobustScaler(inputCol="features_to_scale", outputCol="scaled_features")
    scaler_model = scaler.fit(df_assembled)
    scaled_data = scaler_model.transform(df_assembled)
    scaled_data = scaled_data.drop("features_to_scale")

    vector_to_array = udf(lambda vec: vec.toArray().tolist(), ArrayType(FloatType()))
    scaled_data = scaled_data.withColumn("scaled_columns", vector_to_array(scaled_data["scaled_features"]))
    scaled_data = scaled_data.drop("scaled_features")

    for i, col_name in enumerate(numeric_cols):
        scaled_data = scaled_data.drop(col_name)
        scaled_data = scaled_data.withColumn(col_name, scaled_data["scaled_columns"][i])

    scaled_data = scaled_data.drop("scaled_columns")
    
    return scaled_data

def encode_string_variables(string_cols, df):
    df_to_encode = df
    for col_name in string_cols:
        if col_name == "network_name":
            df_to_encode = df_to_encode.withColumn("network_name", when(col("network_name") == "ethereum", False).otherwise(True))
        else:
            df_to_encode = df_to_encode.drop(col_name) #transaction_id would be dropped anyway, others are deleted till someone find solution
        
    return df_to_encode


def convert_datetime_to_unixtime(datetime_cols, df):
    df_converted = df
    for col_name in datetime_cols:
        df_converted = df_converted.withColumn(col_name, unix_timestamp(col_name))
    return df_converted


def prepare_features(transactions_df, aggregations_df, cols_dict):
    transactions_df = convert_datetime_to_unixtime(cols_dict["transactions_datetime"], transactions_df)
    aggregations_df = convert_datetime_to_unixtime(cols_dict["aggregations_datetime"], aggregations_df).drop("network_name")

    transactions_aggregations_df = join_transactions_with_aggregations(transactions_df, aggregations_df, cols_dict)
    transactions_aggregations_df = encode_string_variables(cols_dict["transactions_string"], transactions_aggregations_df)   

    return transactions_aggregations_df

spark = (
    SparkSession.builder.appName("FeaturesPreprocessing")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "true") # No need as we explicitly specify the schema
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bdp-wallets-aggregations/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.glue_catalog.glue.id", "982534349340") \
    .config("spark.sql.adaptive.enabled", "true") # Keep partitions in simmilar size
    .getOrCreate()
)

setup_blockchain_db(spark)
setup_iceberg_table(spark)

glueContext = GlueContext(spark)
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

transactions_btc_df = glueContext.create_data_frame.from_catalog(
    database="bdp",
    table_name="cleaned_transactions",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
).filter(col("network_name") == "bitcoin")

transactions_eth_df = glueContext.create_data_frame.from_catalog(
    database="bdp",
    table_name="cleaned_transactions",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
).filter(col("network_name") == "ethereum")

aggregations_btc_df = glueContext.create_data_frame.from_catalog(
    database="bdp",
    table_name="wallets_aggregations",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
).filter(col("network_name") == "bitcoin")

aggregations_eth_df = glueContext.create_data_frame.from_catalog(
    database="bdp",
    table_name="wallets_aggregations",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
).filter(col("network_name") == "ethereum")


transactions_aggregations_btc_df = prepare_features(transactions_btc_df, aggregations_btc_df, cols_dict)
glueContext.write_data_frame.from_catalog(
    frame=transactions_aggregations_btc_df,
    database="bdp",
    table_name="unscaled_features",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
)

transactions_aggregations_btc_scaled_df = scale_numeric_variables(cols_dict["transactions_numeric"] + cols_dict["aggregations_numeric"], transactions_aggregations_btc_df)    
glueContext.write_data_frame.from_catalog(
    frame=transactions_aggregations_btc_scaled_df,
    database="bdp",
    table_name="scaled_features",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
)

transactions_aggregations_eth_df = prepare_features(transactions_eth_df, aggregations_eth_df, cols_dict)
glueContext.write_data_frame.from_catalog(
    frame=transactions_aggregations_eth_df,
    database="bdp",
    table_name="unscaled_features",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
)

transactions_aggregations_eth_scaled_df = scale_numeric_variables(cols_dict["transactions_numeric"] + cols_dict["aggregations_numeric"], transactions_aggregations_eth_df)
glueContext.write_data_frame.from_catalog(
    frame=transactions_aggregations_eth_scaled_df,
    database="bdp",
    table_name="scaled_features",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
)

job.commit()
spark.stop()