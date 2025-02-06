import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Authenticate with BigQuery
secrets_client = boto3.client('secretsmanager')
secret = secrets_client.get_secret_value(SecretId='gcp-service-account-key')
service_account_info = json.loads(secret['SecretString'])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
bigquery_client = bigquery.Client(credentials=credentials, project=service_account_info['project_id'])

# Read transformed data from S3
transformed_bucket = "transformed-data-bucket"
transformed_key = f"transformed/{args['date']}.parquet"
transformed_df = spark.read.parquet(f"s3://{transformed_bucket}/{transformed_key}")

# Add date column for partitioning
transformed_df = transformed_df.withColumn("date", lit(args['date']))

# Write to BigQuery
table_id = "your-project-id.etl_dataset.processed_data"
transformed_df.write.format("bigquery") \
    .option("credentials", json.dumps(service_account_info)) \
    .option("table", table_id) \
    .option("partitionField", "date") \
    .mode("append") \
    .save()