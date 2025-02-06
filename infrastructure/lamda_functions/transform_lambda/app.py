import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Extract date from event
    date = event.get('date')
    if not date:
        return {"status": "error", "message": "Date parameter is required"}

    # Read raw data from S3
    bucket_name = "raw-data-bucket"
    file_key = f"raw/{date}.json"
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        raw_data = json.loads(obj['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return {"status": "error", "message": f"No raw data found for {date}"}

    # Transform data
    df = pd.DataFrame(raw_data)
    df['new_column'] = df['old_column'].apply(lambda x: x.upper())

    # Write transformed data to S3
    transformed_bucket = "transformed-data-bucket"
    transformed_key = f"transformed/{date}.parquet"
    df.to_parquet(f"s3://{transformed_bucket}/{transformed_key}")

    return {"status": "success", "message": f"Data transformed for {date}"}