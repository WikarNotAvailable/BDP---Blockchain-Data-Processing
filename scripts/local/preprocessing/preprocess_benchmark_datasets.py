from pyspark.sql import SparkSession
from scripts.local.shared.benchmark_schemas import benchmark_transaction_schema, benchmark_aggregations_schema
from scripts.local.preprocessing.components.preprocess_data_first_step import first_preprocess_testing_data
from scripts.local.preprocessing.components.preprocess_data_second_step import second_preprocess_testing_data
from scripts.local.preprocessing.components.preprocess_data_third_step import third_preprocess_testing_data

spark = (
    SparkSession.builder.appName("DataPreprocessing")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "6g")  # Increase executor memory
    .config("spark.driver.memory", "2g")    # Increase driver memory
    #.config("spark.local.dir", "/mnt/d/spark-temp")  # Change temp directory
    .getOrCreate()
)

transactions_df = spark.read.schema(benchmark_transaction_schema).parquet("data/benchmark/etl/transactions")

aggregations_df = spark.read.schema(benchmark_aggregations_schema).parquet("data/benchmark/aggregations")

first_preprocess_testing_data(transactions_df, aggregations_df)
df = second_preprocess_testing_data(spark)
third_preprocess_testing_data(df)