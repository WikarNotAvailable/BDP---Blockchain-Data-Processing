from pyspark.sql import SparkSession
from scripts.local.shared.schemas import transaction_schema
from scripts.local.aggregate.components.aggregations import aggregate

spark = (
    SparkSession.builder.appName("DataAggregations")    
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "6g")
    .config("spark.driver.memory", "2g")
    # .config("spark.local.dir", "/mnt/d/spark-temp") # Change the temp directory
    .getOrCreate()
)

source_dir = "data/historical/etl/transactions"
output_dir = "data/historical/aggregations" 

aggregate(spark, source_dir, output_dir, transaction_schema)