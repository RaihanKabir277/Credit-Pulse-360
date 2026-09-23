# Databricks notebook source
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="************",
    aws_access_key_id="************",
    aws_secret_access_key="************",
    region_name="************"
)

# Test the connection
response = s3.list_objects_v2(
    Bucket="creditpulse",
    Prefix="data/",
    MaxKeys=10
)

print("Connected to MinIO successfully!")
print("Objects:")

for obj in response.get("Contents", []):
    print("-", obj["Key"])

# COMMAND ----------

import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="************",
    aws_access_key_id="************",
    aws_secret_access_key="************",
    region_name="************"
)

print("MinIO client created successfully!")

# COMMAND ----------

import pandas as pd
from io import BytesIO

response = s3.get_object(
    Bucket="creditpulse",
    Key="data/customers.csv"
)

pdf_customers = pd.read_csv(
    BytesIO(response["Body"].read())
)

df_customers = spark.createDataFrame(pdf_customers)

print("Successfully loaded customers.csv into Spark!")
print("Rows:", df_customers.count())
print("Columns:", df_customers.columns)

display(df_customers)

# COMMAND ----------

s3.copy_object(
    Bucket="bronze",
    CopySource={
        "Bucket": "creditpulse",
        "Key": "data/customers.csv"
    },
    Key="creditpulse/customers.csv"
)

print("customers.csv successfully copied to Bronze!")

# COMMAND ----------

import pandas as pd
from io import BytesIO

response = s3.get_object(
    Bucket="creditpulse",
    Key="data/repayment_schedule.csv"
)

pdf_repayment_schedule = pd.read_csv(
    BytesIO(response["Body"].read())
)

df_repayment_schedule = spark.createDataFrame(pdf_repayment_schedule)

print("Successfully loaded repayment_schedule.csv into Spark!")
print("Rows:", df_repayment_schedule.count())
print("Columns:", df_repayment_schedule.columns)

display(df_repayment_schedule)

# COMMAND ----------

s3.copy_object(
    Bucket="bronze",
    CopySource={
        "Bucket": "creditpulse",
        "Key": "data/repayment_schedule.csv"
    },
    Key="creditpulse/repayment_schedule.csv"
)

print("epayment_schedule.csv successfully copied to Bronze!")

# COMMAND ----------

s3.copy_object(
    Bucket="bronze",
    CopySource={
        "Bucket": "creditpulse",
        "Key": "data/loan_applications.csv"
    },
    Key="creditpulse/loan_applications.csv"
)

print("loan_applications.csv successfully copied to Bronze!")

# COMMAND ----------

files = [
    "loan_accounts.csv",
    "credit_bureau_reports.csv",
    "collections_delinquency.csv"
]

for file in files:
    s3.copy_object(
        Bucket="bronze",
        CopySource={
            "Bucket": "creditpulse",
            "Key": f"data/{file}"
        },
        Key=f"creditpulse/{file}"
    )

print("All files successfully copied to Bronze!")

# COMMAND ----------

import pandas as pd
from io import BytesIO

response = s3.get_object(
    Bucket="bronze",
    Key="creditpulse/credit_bureau_reports.csv"
)

df_credit_bureau = pd.read_csv(
    BytesIO(response["Body"].read())
)

print("Successfully loaded credit_bureau_reports.csv from Bronze!")
print("Rows:", len(df_credit_bureau))
print("Columns:", df_credit_bureau.columns.tolist())

display(df_credit_bureau)

# COMMAND ----------

import pandas as pd
from io import BytesIO

response = s3.get_object(
    Bucket="bronze",
    Key="creditpulse/collections_delinquency.csv"
)

df_collections_delinquency = pd.read_csv(
    BytesIO(response["Body"].read())
)

print("Successfully loaded collections_delinquency.csv from Bronze!")
print("Rows:", len(df_collections_delinquency))
print("Columns:", df_collections_delinquency.columns.tolist())

display(df_collections_delinquency)

# COMMAND ----------

import pandas as pd
from io import BytesIO

response = s3.get_object(
    Bucket="bronze",
    Key="creditpulse/loan_accounts.csv"
)

df_loan_accounts = pd.read_csv(
    BytesIO(response["Body"].read())
)

print("Successfully loaded from Bronze!")
print("Rows:", len(df_loan_accounts))
print("Columns:", df_loan_accounts.columns.tolist())

display(df_loan_accounts)
