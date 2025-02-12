import boto3
import os
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Specify the source and destination buckets and directories
    source_bucket = 'upload-raw-data'
    source_prefix = 'raw-data/'  # Updated to match your description

    destination_bucket = 'serverless-etl'
    destination_prefix = 'input-data'

    try:
        # List objects in the source directory
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                # Get the object key (file path)
                source_key = obj['Key']
                
                # Skip if the key is a directory (ends with '/')
                if source_key.endswith('/'):
                    continue
                
                # Extract the file name (without the prefix)
                
                # Get the current date in YYYY/MM/DD format
                current_date = datetime.now().strftime("%Y-%m-%d")
                
                # Create the destination key with the date format
                destination_key = f"{destination_prefix}/data_{current_date}"
                
                # Copy the object to the destination
                copy_source = {'Bucket': source_bucket, 'Key': source_key}
                s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
                
                # Delete the object from the source (if you want to move instead of copy)
                # s3.delete_object(Bucket=source_bucket, Key=source_key)
                
                print(f"Moved {source_key} to {destination_key}")
        else:
            print("No files found in the source directory.")
        
        return {
            'statusCode': 200,
            "bucket_name" : destination_bucket,
            "destination_key" : f"{destination_prefix}",
            'body': 'Files moved successfully!'
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }