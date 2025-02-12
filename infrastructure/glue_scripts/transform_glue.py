import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, count, to_date, datediff
import boto3
import json
import re
from datetime import datetime

args = getResolvedOptions(sys.argv, ['destination_bucket', 'destination_key'])

# Extract destination_bucket and destination_key
destination_bucket = args['destination_bucket']
destination_key = args['destination_key']

# Debugging: Print the extracted values
print(f"Destination Bucket: {destination_bucket}")
print(f"Destination Key: {destination_key}")

# Debugging: Print the extracted values
print(f"Destination Bucket: {destination_bucket}")
print(f"Destination Key: {destination_key}")
destination_key = args.get("destination_key")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
s3_client = boto3.client("s3")

def extract_date_from_filename(filename):
    print("fileName:", filename)
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    if match:
        return datetime.strptime(match.group(), "%Y-%m-%d")
    else:
        raise ValueError(f"Filename '{filename}' does not contain a valid date.")

# Get the most recent file from S3
def get_most_recent_file(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    print("Response:", response)

    if 'Contents' not in response:
        raise Exception(f"No files found in s3://{bucket}/{prefix}")

    # Filter out directory-like keys and select files containing dates
    files = [
        (obj['Key'], extract_date_from_filename(obj['Key']))
        for obj in response['Contents']
        if not obj['Key'].endswith("/")  # Skip folder keys
    ]

    if not files:
        raise Exception(f"No valid files found in s3://{bucket}/{prefix}")

    # Find the most recent file
    most_recent_file = max(files, key=lambda x: x[1])[0]
    return f"s3://{bucket}/{most_recent_file}"

# Configuration for S3 paths
bucket_name = destination_bucket
input_prefix = destination_key
output_prefix = 'transformed-data/'

# Get the most recent file path
most_recent_file_path = get_most_recent_file(bucket_name, input_prefix)
print(f"Most recent file found: {most_recent_file_path}")

# Load the data using Spark
df = spark.read.csv(
    most_recent_file_path.replace("s3://", f"s3a://"),
    header=True,
    inferSchema=True
)

# Select columns
df_clean = df.toDF(*[col_name.strip().replace(" ", "_") for col_name in df.columns])

# Report 1: Hospital-Wise Admission Counts
hospital_admission_counts = df_clean.groupBy("Hospital").agg(count("*").alias("Total_Admissions"))
hospital_admission_counts.write.mode("overwrite").parquet("s3a://serverless-etl/transformed-data/hospital_admissions.parquet")

print(hospital_admission_counts.show())
# Report 2: Average Billing Amount by Hospital
avg_billing_by_hospital = df_clean.groupBy("Hospital").agg(avg("Billing_Amount").alias("Avg_Billing_Amount"))
avg_billing_by_hospital.write.mode("overwrite").parquet("s3a://serverless-etl/transformed-data/avg_billing.parquet")

# Report 3: Top Doctors with Highest Patient Counts
top_doctors = df_clean.groupBy("Doctor").agg(count("*").alias("Patient_Count")).orderBy(col("Patient_Count").desc())
top_doctors.write.mode("overwrite").parquet("s3a://serverless-etl/transformed-data/top_doctors.parquet")

# Report 4: Medical Condition Statistics
condition_stats = df_clean.groupBy("Medical_Condition").agg(count("*").alias("Condition_Count"))
condition_stats.write.mode("overwrite").parquet("s3a://serverless-etl/transformed-data/condition_stats.parquet")

# Report 5: Gender Distribution by Hospital
gender_distribution = df_clean.groupBy("Hospital", "Gender").agg(count("*").alias("Gender_Count"))
gender_distribution.write.mode("overwrite").parquet("s3a://serverless-etl/transformed-data/gender_distribution.parquet")

# Report 6: Admission Duration (Length of Stay) Insights
df_clean = df_clean.withColumn("Date_of_Admission", to_date("Date_of_Admission", "yyyy-MM-dd")) \
                   .withColumn("Discharge_Date", to_date("Discharge_Date", "yyyy-MM-dd"))

length_of_stay = df_clean.withColumn("Length_of_Stay", datediff("Discharge_Date", "Date_of_Admission")) \
                         .groupBy("Hospital").agg(avg("Length_of_Stay").alias("Avg_Length_of_Stay"))
length_of_stay.write.mode("overwrite").parquet("s3a://serverless-etl/transformed-data/length_of_stay.parquet")

print("All reports have been generated and saved to S3.")
spark.stop()