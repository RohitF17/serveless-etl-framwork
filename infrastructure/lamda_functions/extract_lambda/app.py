import boto3
import requests
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Extract date from event
    date = event.get('date')
    if not date:
        return {"status": "error", "message": "Date parameter is required"}

    # Validate date format
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD"}

    # Check if S3 file for the date already exists
    bucket_name = "raw-data-bucket"
    file_key = f"raw/{date}.json"
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return {"status": "error", "message": f"File for {date} already exists"}
    except s3_client.exceptions.NoSuchKey:
        pass  # File does not exist, proceed

    # Extract data from API
    api_url = f"https://api.example.com/data?date={date}"
    response = requests.get(api_url)
    if response.status_code != 200:
        return {"status": "error", "message": "Failed to fetch data from API"}

    # Write raw data to S3
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=response.text)

    return {"status": "success", "message": f"Data extracted for {date}"}