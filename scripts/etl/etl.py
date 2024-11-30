from functools import reduce
import datetime
from typing import Callable
from pyspark.sql import SparkSession, DataFrame
from utils import download_files, get_s3_objects


def extract(s3: str, bucket_name: str, prefix:str, days, save_dir: str) -> list[str]:
    current_date = datetime.datetime.now(datetime.timezone.utc)
    days_to_download = current_date - datetime.timedelta(days=days)

    objects = get_s3_objects(s3, bucket_name, prefix)
    recent_files = [obj for obj in objects if obj["LastModified"] > days_to_download]
    recent_files.sort(key=lambda x: x["LastModified"], reverse=True)
    transaction_files_names = download_files(s3, bucket_name, recent_files, save_dir)

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
    output_dir = f"results/{dir}"  
    if not merged_transactions_df.isEmpty():
        merged_transactions_df.coalesce(1).write.parquet(output_dir, mode="overwrite", compression="zstd")

        print(f"Transformed data saved to {output_dir}")
    else:
        print("No data found within the specified time range.")