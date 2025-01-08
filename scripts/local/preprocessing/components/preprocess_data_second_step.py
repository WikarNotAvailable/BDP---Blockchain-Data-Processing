from scripts.local.preprocessing.components.join_transactions_with_aggregations import join_transactions_with_aggregations
from scripts.local.shared.schemas import transaction_scaled_schema, aggregations_scaled_schema
from scripts.local.shared.benchmark_schemas import benchmark_transaction_scaled_schema, benchmark_aggregations_scaled_schema

def second_preprocess_training_data(spark):     
    join_transactions_with_aggregations(spark, 
                                        "data/historical/preprocessing/transactions_merged", 
                                        "data/historical/preprocessing/aggregations_merged", 
                                        "data/historical/preprocessing/merged_networks_transactions_with_aggregations",
                                        transaction_scaled_schema,
                                        aggregations_scaled_schema)
    
    join_transactions_with_aggregations(spark, 
                                        "data/historical/preprocessing/transactions_eth", 
                                        "data/historical/preprocessing/aggregations_eth", 
                                        "data/historical/preprocessing/eth_with_aggregations",
                                        transaction_scaled_schema,
                                        aggregations_scaled_schema)
    
    join_transactions_with_aggregations(spark, 
                                        "data/historical/preprocessing/transactions_btc", 
                                        "data/historical/preprocessing/aggregations_btc", 
                                        "data/historical/preprocessing/btc_with_aggregations",
                                        transaction_scaled_schema,
                                        aggregations_scaled_schema)

def second_preprocess_testing_data(spark):     
    return join_transactions_with_aggregations(spark, 
                                               "data/benchmark/preprocessing/transactions_eth", 
                                               "data/benchmark/preprocessing/aggregations_eth", 
                                               "data/benchmark/preprocessing/eth_with_aggregations",
                                               benchmark_transaction_scaled_schema,
                                               benchmark_aggregations_scaled_schema)