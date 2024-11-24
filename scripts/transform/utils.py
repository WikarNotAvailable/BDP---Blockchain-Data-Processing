import os
import datetime


def get_s3_objects(s3, bucket, prefix):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )
    return response.get("Contents", [])


def download_last_7_parquets(s3, bucket_name, prefix, data_dir):
    current_date = datetime.datetime.now(datetime.timezone.utc)
    seven_days_ago = current_date - datetime.timedelta(days=7)

    transaction_files = []

    objects = get_s3_objects(s3, bucket_name, prefix)

    recent_files = [obj for obj in objects if obj["LastModified"] > seven_days_ago]

    recent_files.sort(key=lambda x: x["LastModified"], reverse=True)

    for obj in recent_files[:7]:  # Download only the top 7 files
        file_name = os.path.basename(obj["Key"])
        file_path = os.path.join(data_dir, file_name)

        os.makedirs(data_dir, exist_ok=True)

        print(f"Downloading {file_name}...")
        try:
            if os.path.exists(file_path):
                transaction_files.append(file_path)
                print(f"{file_name} already exists. Skipping...")
                continue

            s3.download_file(bucket_name, obj["Key"], file_path)
            transaction_files.append(file_path)
            print(f"Downloaded {file_name} to {file_path}.")
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")

    return transaction_files
