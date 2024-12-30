import os
import datetime
import re

def get_s3_objects(s3, bucket, prefix, start_date):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        StartAfter = f"{prefix}date={start_date}"
    )
    return response.get("Contents", [])


def download_files(s3: str, bucket_name: str, files_to_download, temp_dir) -> list[str]:
    transaction_files_names = []

    for obj in files_to_download:
        file_name = os.path.basename(obj["Key"])
        file_path = os.path.join(temp_dir, file_name)

        os.makedirs(temp_dir, exist_ok=True)

        print(f"Downloading {file_name}...")
        try:
            if os.path.exists(file_path):
                transaction_files_names.append(file_path)
                print(f"{file_name} already exists. Skipping...")
                continue

            s3.download_file(bucket_name, obj["Key"], file_path)
            transaction_files_names.append(file_path)
            print(f"Downloaded {file_name} to {file_path}.")
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")

    return transaction_files_names


def filter_files_by_date(file_names: str, start_date: str, end_date: str) -> list[str]:
    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    regex = r'date=(\d{4}-\d{2}-\d{2})'

    filtered_file_names = []
    for file_name in file_names:
        match = re.search(regex, file_name['Key'])
        if match:
            file_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d")
            if start_datetime <= file_date <= end_datetime:
                filtered_file_names.append(file_name)

    return filtered_file_names
