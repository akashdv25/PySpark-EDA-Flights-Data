import os
from dotenv import load_dotenv
import boto3
from pathlib import Path
from datetime import datetime
import uuid

# Load environment variables from .env file
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# List of files to upload: (local_path, s3_key)
FILES = [
    ("/home/new-user/Documents/PySpark-EDA-Flights-Data/data/small_flights_df.csv", "small_flights_df.csv"),
    ("/home/new-user/Documents/PySpark-EDA-Flights-Data/data/airlines.csv", "airlines.csv"),
    ("/home/new-user/Documents/PySpark-EDA-Flights-Data/data/airports.csv", "airports.csv"),
]

def upload_file(local_path, s3_key):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3.upload_file(local_path, S3_BUCKET_NAME, s3_key)
    print(f"Uploaded {local_path} to s3://{S3_BUCKET_NAME}/{s3_key}")




def save_df_to_s3_parquet(df):
    """
    Saves a PySpark DataFrame as a Parquet file directly to S3.
    Uses environment variables for AWS credentials and bucket info.
    The S3 key will be 'parquet_export_<timestamp>_<uuid>/'.
    """
    # Load environment variables
    load_dotenv()
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    # Generate unique S3 folder/key
    unique_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]
    s3_path = f"s3a://{S3_BUCKET_NAME}/parquet_export_{unique_id}/"

    # Write DataFrame directly to S3 as Parquet
    df.write.mode("overwrite").parquet(s3_path)
    print(f"DataFrame saved directly to {s3_path} as Parquet.")






if __name__ == "__main__":
    for local_path, s3_key in FILES:
        if Path(local_path).exists():
            upload_file(local_path, s3_key)
        else:
            print(f"File not found: {local_path}")