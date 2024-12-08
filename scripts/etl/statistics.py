from pyspark.sql import SparkSession, DataFrame
from schemas import transaction_schema, aggregations_schema

spark = (
    SparkSession.builder.appName("DataAggregations")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "16g")  # Increase executor memory
    .config("spark.driver.memory", "16g")    # Increase driver memory
    #.config("spark.local.dir", "/mnt/d/spark-temp")  # Change temp directory
    .getOrCreate()
)

transaction_df = spark.read.schema(transaction_schema).parquet("results/transaction")
aggregations_df = spark.read.schema(aggregations_schema).parquet("results/aggregations")

#features_df = transaction_df.join(aggregations_df, "address", "left")

aggregations_df.show(n=10, vertical=True, truncate=False)

spark.stop()