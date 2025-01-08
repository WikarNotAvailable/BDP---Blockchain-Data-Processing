from functools import reduce
from typing import Callable
from pyspark.sql import SparkSession, DataFrame
from scripts.local.shared.benchmark_schemas import benchmark_input_schema

def extract(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.schema(benchmark_input_schema).csv(path, header=True)

    return df


def transform(spark: SparkSession, 
                df: DataFrame, 
                transformation: Callable[[SparkSession, DataFrame], DataFrame] 
                ) -> None: 

    df = transformation(spark, df)

    return df

    
def load(merged_transactions_df: DataFrame, dir: str) -> None:
    output_dir = f"data/benchmark/etl/{dir}"  
    if not merged_transactions_df.isEmpty():
        merged_transactions_df.coalesce(1).write.parquet(output_dir, mode="overwrite", compression="zstd")

        print(f"Transformed data saved to {output_dir}")
    else:
        print("No data found.")
