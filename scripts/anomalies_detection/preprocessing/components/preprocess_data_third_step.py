from pyspark.sql.functions import col, when
from scripts.shared.consts import aggregated_transactions_string_cols
from scripts.anomalies_detection.preprocessing.components.preprocess_data_first_step import save_preprocessed_data


'''    
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
'''

def encode_string_variables(string_cols, df):
    df_to_encode = df
    for i, col_name in enumerate(string_cols):
        if col_name == "network_name":
            df_to_encode = df_to_encode.withColumn("network_name", when(col("network_name") == "ethereum", False).otherwise(True))
        else:
            #transaction_id would be dropped anyway, others are deleted till someone find solution
            df_to_encode = df_to_encode.drop(col_name)
        
    return df_to_encode

def third_preprocess_training_data(aggregated_transactions_merged_df, aggregated_transactions_eth_df, aggregated_transactions_btc_df):     
    aggregated_transactions_merged_df = encode_string_variables(aggregated_transactions_string_cols, aggregated_transactions_merged_df)
    save_preprocessed_data(aggregated_transactions_merged_df, "data/historical/training/merged")
    
    aggregated_transactions_eth_df = encode_string_variables(aggregated_transactions_string_cols, aggregated_transactions_eth_df)
    save_preprocessed_data(aggregated_transactions_eth_df, "data/historical/training/eth")
    
    aggregated_transactions_btc_df = encode_string_variables(aggregated_transactions_string_cols, aggregated_transactions_btc_df)
    save_preprocessed_data(aggregated_transactions_btc_df, "data/historical/training/btc")
    
def third_preprocess_testing_data(aggregated_transactions_eth_df):     
    aggregated_transactions_eth_df = encode_string_variables(aggregated_transactions_string_cols, aggregated_transactions_eth_df)
    save_preprocessed_data(aggregated_transactions_eth_df, "data/benchmark/testing/eth")
