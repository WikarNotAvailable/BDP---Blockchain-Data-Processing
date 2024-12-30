from functools import reduce
from typing import Callable
from pyspark.sql import SparkSession, DataFrame
from scripts.local.etl.components.utils import download_files, get_s3_objects, filter_files_by_date


def extract(s3: str, bucket_name: str, prefix:str, start_date: str, end_date: str, save_dir: str) -> list[str]:
    file_names = get_s3_objects(s3, bucket_name, prefix, start_date)
    filltered_file_names = filter_files_by_date(file_names, start_date, end_date)
    transaction_files_names = download_files(s3, bucket_name, filltered_file_names, save_dir)

    return transaction_files_names


def transform(spark: SparkSession, 
                transaction_files: list[str], 
                transformation: Callable[[SparkSession, str], None] 
                ) -> None: 

    transaction_df_list = []
    merged_transactions_df = None

    for file_name in transaction_files:
        print(f"Processing file: {file_name}")
        transformed_data = transformation(spark, file_name)
        transaction_df_list.append(transformed_data)

    if transaction_df_list:
        merged_transactions_df = reduce(DataFrame.union, transaction_df_list) # For large datasets it may be better to do incremental union in a loop

    return merged_transactions_df

    
def load(merged_transactions_df: DataFrame, dir: str) -> None:
    output_dir = f"data/historical/etl/{dir}"  
    if not merged_transactions_df.isEmpty():
        merged_transactions_df.coalesce(1).write.parquet(output_dir, mode="overwrite", compression="zstd")

        print(f"Transformed data saved to {output_dir}")
    else:
        print("No data found within the specified time range.")
