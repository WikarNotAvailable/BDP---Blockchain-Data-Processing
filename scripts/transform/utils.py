import os


def get_s3_objects(s3, bucket, prefix):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )
    return response.get("Contents", [])


def download_files(s3: str, bucket_name: str, files_to_download, temp_dir: str = "temp_data") -> list[str]:
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
