## Cost-Effective Serverless ETL Pipeline

## Overview
This project demonstrates a fully serverless ETL pipeline that extracts data via an API trigger, transforms it using AWS Glue, stores the results in Amazon S3 as Parquet files, and then loads the transformed data into Google BigQuery for analytics.

## Architecture

The architecture consists of the following AWS services:

**AWS API Gateway**: Provides a RESTful API to trigger the ETL pipeline.

**AWS Lambda**: Acts as the entry point, receiving API requests and triggering the AWS Step Function.

**AWS Step Functions**: Orchestrates the ETL pipeline workflow.

**AWS Glue**: Processes and transforms raw data into Parquet format.

**Amazon S3**: Stores the transformed Parquet files.

**Google BigQuery**: Performs analytics on the transformed data via a data transfer job.

## Workflow

1. A client sends a POST request to the API Gateway endpoint to initiate the ETL process.

2. The API Gateway triggers an AWS Step Function execution.
![image](https://github.com/user-attachments/assets/f837b4e3-e843-483a-9118-e9b7b4317af8)

3. The AWS Step Function execution starts an Lambda Function.
![image](https://github.com/user-attachments/assets/6094b15f-590c-437e-b60f-a313ea6fef2e)


4. The Step Function invokes an AWS Glue job to process and transform data.
![image](https://github.com/user-attachments/assets/fda9d338-02a6-491c-a6a9-56a53baaec1e)

5. The Glue job writes the output in Parquet format to an Amazon S3 bucket.
![image](https://github.com/user-attachments/assets/93df5c48-1fb1-4d82-a134-04003b1893ce)

6. A Google BigQuery Data Transfer Service job picks up the transformed data from S3 and loads it into BigQuery for analytics.
![image](https://github.com/user-attachments/assets/f03222f8-c83b-4928-bb99-5e74788dda77)


## Cost Efficiency
By leveraging AWS serverless services, the pipeline remains highly cost-efficient:

**Pay-per-use model**: AWS Lambda and Glue only incur costs when they execute, eliminating the need for always-on infrastructure.

**S3 storage efficiency**: Storing transformed data as Parquet significantly reduces storage costs due to compression and optimized read performance.

**Step Functions orchestration**: Eliminates the need for custom scheduling and monitoring logic, reducing maintenance overhead.

**Serverless Google BigQuery transfer**: Removes the need for complex data ingestion pipelines, further lowering operational costs.

**No upfront provisioning**: Unlike traditional ETL tools, this setup avoids upfront infrastructure investments.


## Setup Instructions

## Prerequisites
Ensure you have the following installed and configured:
**AWS CLI
Google Cloud SDK**

AWS Setup
1** .Create an S3 bucket** to store transformed data:
```
aws s3 mb s3://your-etl-bucket

```
2. **Deploy the CloudFormation template **to set up API Gateway, Lambda, Step Functions, and Glue:
```
aws cloudformation deploy --template-file template.yaml --stack-name etl-pipeline --capabilities CAPABILITY_NAMED_IAM

```
3 .Deploy the Lambda function:
```
zip function.zip lambda_function.py
aws lambda create-function --function-name startETL --runtime python3.8 --role arn:aws:iam::your-account-id:role/lambda-role --handler lambda_function.lambda_handler --zip-file fileb://function.zip
```

**Google BigQuery Setup**

1. Enable the Google BigQuery Data Transfer Service in your Google Cloud project.

2. Set up a transfer configuration in BigQuery to pull data from S3.

  1. Navigate to BigQuery > Data Transfers.
  
  2. Select Amazon S3 as the source.
  
  3. Provide the S3 bucket details and access credentials.
  
  4. Configure the dataset and schedule.


## Deployment

1. Deploy the AWS CloudFormation template to set up the infrastructure.

2. Configure Google BigQuery Data Transfer Service to pull data from Amazon S3.

3. Test the API Gateway endpoint to initiate the ETL pipeline.
  
