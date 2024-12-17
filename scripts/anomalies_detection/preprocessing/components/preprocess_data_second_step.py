from scripts.anomalies_detection.preprocessing.components.join_transactions_with_aggregations import join_transactions_with_aggregations

def second_preprocess_training_data(spark):     
    join_transactions_with_aggregations(spark, "data/historical/preprocessing/transactions_merged", "data/historical/preprocessing/aggregations_merged", "data/historical/preprocessing/merged_networks_transactions_with_aggregations")
    
    join_transactions_with_aggregations(spark, "data/historical/preprocessing/transactions_eth", "data/historical/preprocessing/aggregations_eth", "data/historical/preprocessing/eth_with_aggregations")
    
    join_transactions_with_aggregations(spark, "data/historical/preprocessing/transactions_btc", "data/historical/preprocessing/aggregations_btc", "data/historical/preprocessing/btc_with_aggregations")

def second_preprocess_testing_data(spark):     
    return join_transactions_with_aggregations(spark, "data/benchmark/preprocessing/transactions_eth", "data/benchmark/preprocessing/aggregations_eth", "data/benchmark/preprocessing/eth_with_aggregations")
