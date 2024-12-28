from pyspark.sql.functions import udf, col, when
from pyspark.ml.feature import VectorAssembler, RobustScaler, StringIndexer
from scripts.local.shared.consts import transaction_numeric_cols, aggregation_numeric_cols, transactions_datetime_cols, aggregations_datetime_cols
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql.functions import unix_timestamp

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
    for i, col_name in enumerate(string_cols):
        if col_name == "network_name":
            df_to_encode = df_to_encode.withColumn("network_name", when(col("network_name") == "ethereum", False).otherwise(True))
        else:
            indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_encoded")
            index_model = indexer.fit(df_to_encode)
            df_to_encode = index_model.transform(df_to_encode)
        
            df_to_encode = df_to_encode.withColumn(col_name, col(f"{col_name}_encoded"))
            df_to_encode = df_to_encode.drop(f"{col_name}_encoded")
        
    return df_to_encode

def scale_datetime_variables(datetime_cols, df):
    df_to_scale = df
    for i, col_name in enumerate(datetime_cols):
        df_to_scale = df_to_scale.withColumn(col_name, unix_timestamp(col_name))
        
    scaled_data = scale_numeric_variables(datetime_cols, df_to_scale)
    
    return scaled_data

def save_preprocessed_data(df, output_dir):
    df.coalesce(1).write.parquet(output_dir, mode="overwrite", compression="zstd")

def first_preprocess_training_data(transactions_eth_df, transactions_btc_df, aggregations_eth_df, aggregations_btc_df):     
    transactions_ethereum_df = scale_numeric_variables(transaction_numeric_cols, transactions_eth_df)
    transactions_ethereum_df = scale_datetime_variables(transactions_datetime_cols, transactions_ethereum_df)
    aggregations_ethereum_df = scale_numeric_variables(aggregation_numeric_cols, aggregations_eth_df)
    aggregations_ethereum_df = scale_datetime_variables(aggregations_datetime_cols, aggregations_ethereum_df)
    save_preprocessed_data(transactions_ethereum_df, "data/historical/preprocessing/transactions_eth")
    save_preprocessed_data(aggregations_ethereum_df, "data/historical/preprocessing/aggregations_eth")
    
    transactions_bitcoin_df = scale_numeric_variables(transaction_numeric_cols, transactions_btc_df)
    transactions_bitcoin_df = scale_datetime_variables(transactions_datetime_cols, transactions_bitcoin_df)
    aggregations_bitcoin_df = scale_numeric_variables(aggregation_numeric_cols, aggregations_btc_df)
    aggregations_bitcoin_df = scale_datetime_variables(aggregations_datetime_cols, aggregations_bitcoin_df)
    save_preprocessed_data(transactions_bitcoin_df, "data/historical/preprocessing/transactions_btc")
    save_preprocessed_data(aggregations_bitcoin_df, "data/historical/preprocessing/aggregations_btc")

    transactions_df = transactions_bitcoin_df.unionByName(transactions_ethereum_df)
    aggregations_df = aggregations_bitcoin_df.unionByName(aggregations_ethereum_df)
    save_preprocessed_data(transactions_df, "data/historical/preprocessing/transactions_merged")
    save_preprocessed_data(aggregations_df, "data/historical/preprocessing/aggregations_merged")
    
def first_preprocess_testing_data(transactions_eth_df, aggregations_eth_df): 
    transactions_ethereum_df = scale_numeric_variables(transaction_numeric_cols, transactions_eth_df)
    transactions_ethereum_df = scale_datetime_variables(transactions_datetime_cols, transactions_ethereum_df)
    aggregations_ethereum_df = scale_numeric_variables(aggregation_numeric_cols, aggregations_eth_df)
    aggregations_ethereum_df = scale_datetime_variables(aggregations_datetime_cols, aggregations_ethereum_df)
    save_preprocessed_data(transactions_ethereum_df, "data/benchmark/preprocessing/transactions_eth")
    save_preprocessed_data(aggregations_ethereum_df, "data/benchmark/preprocessing/aggregations_eth")
