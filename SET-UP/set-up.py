# Databricks notebook source
import requests

url = "************/minio/health/live"

response = requests.get(url, timeout=10)

print("Status Code:", response.status_code)
print("Response:", response.text)

# COMMAND ----------

import requests

url = "************/bronze"

response = requests.get(url, timeout=10)

print("Status Code:", response.status_code)
print("Response:")
print(response.text[:2000])

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth

url = "************/bronze"

response = requests.get(
    url,
    auth=HTTPBasicAuth("************", "************"),
    timeout=10
)

print("Status Code:", response.status_code)
print("Response:")
print(response.text[:2000])

# COMMAND ----------

import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="************",
    aws_access_key_id="************",
    aws_secret_access_key="************",
    region_name="************",
)

response = s3.list_buckets()

print("Successfully authenticated with MinIO!")
print("Buckets:")

for bucket in response["Buckets"]:
    print("-", bucket["Name"])

# COMMAND ----------

response = s3.list_objects_v2(
    Bucket="bronze",
    MaxKeys=10
)

print("Successfully accessed bronze bucket!")
print("Objects found:", response.get("KeyCount", 0))

for obj in response.get("Contents", []):
    print("-", obj["Key"])

# COMMAND ----------

print("Spark version:", spark.version)

try:
    print("S3A filesystem class:")
    print(spark._jvm.org.apache.hadoop.fs.s3a.S3AFileSystem)
except Exception as e:
    print("S3A is NOT available:")
    print(type(e).__name__, str(e))

# COMMAND ----------

print("Spark Connect session:", type(spark))
print("Spark version:", spark.version)

for key in [
    "spark.hadoop.fs.s3a.endpoint",
    "spark.hadoop.fs.s3a.access.key",
    "spark.hadoop.fs.s3a.impl",
]:
    try:
        print(key, "=", spark.conf.get(key))
    except Exception as e:
        print(key)

# COMMAND ----------

import pyarrow as pa
import pyarrow.parquet as pq

print("PyArrow version:", pa.__version__)

# COMMAND ----------

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# MinIO connection
s3 = boto3.client(
    "s3",
    endpoint_url="************",
    aws_access_key_id="************",
    aws_secret_access_key="************",
    region_name="************"
)

# Sample data
table = pa.table({
    "customer_id": [1, 2, 3, 4, 5],
    "customer_name": ["Raihan", "Kabir", "Jilany", "Tabassum", "Taqib"],
    "balance": [1500.50, 2300.75, 875.25, 4200.00, 3150.80]
})

# Create Parquet in memory
buffer = BytesIO()
pq.write_table(table, buffer)
buffer.seek(0)

# Upload to MinIO
s3.put_object(
    Bucket="bronze",
    Key="demo/customers.parquet",
    Body=buffer.getvalue()
)

print("Parquet file uploaded successfully!")
print("MinIO path: bronze/demo/customers.parquet")

# COMMAND ----------

response = s3.list_objects_v2(
    Bucket="bronze",
    Prefix="demo/"
)

print("Objects in bronze/demo/:")

for obj in response.get("Contents", []):
    print(
        f"- {obj['Key']} | "
        f"Size: {obj['Size']} bytes"
    )

# COMMAND ----------

import pyarrow.parquet as pq
from io import BytesIO

# Download the Parquet object from MinIO
response = s3.get_object(
    Bucket="bronze",
    Key="demo/customers.parquet"
)

# Read Parquet directly from memory
table = pq.read_table(
    BytesIO(response["Body"].read())
)

# Convert to pandas for inspection
df_minio = table.to_pandas()

print("Successfully read Parquet from MinIO!")
print("Rows:", len(df_minio))
print("Columns:", list(df_minio.columns))

display(df_minio)

# COMMAND ----------

print(type(spark))

# COMMAND ----------

df_test = spark.createDataFrame([
    (1, "Raihan", 1500.50),
    (2, "Kabir", 2300.75),
    (3, "Jilany", 875.25),
], ["customer_id", "customer_name", "balance"])

df_test.show()

# COMMAND ----------

spark.conf.set(
    "spark.hadoop.fs.s3a.endpoint",
    "************"
)

print(
    spark.conf.get("spark.hadoop.fs.s3a.endpoint")
)

# COMMAND ----------

print("Spark Connect:", type(spark))
print("Spark version:", spark.version)

try:
    print("Catalogs:")
    display(spark.sql("SHOW CATALOGS"))
except Exception as e:
    print("Catalog check failed:")
    print(type(e).__name__, str(e))

# COMMAND ----------

display(
    spark.sql("SHOW SCHEMAS IN customer_360")
)

# COMMAND ----------

display(
    spark.sql("DESCRIBE SCHEMA EXTENDED customer_360.bronze")
)

# COMMAND ----------

display(
    spark.sql("SHOW EXTERNAL LOCATIONS")
)

# COMMAND ----------

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
    Bucket="bronze",
    Prefix="demo/",
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

import pyarrow.parquet as pq
from io import BytesIO

response = s3.get_object(
    Bucket="bronze",
    Key="demo/customers.parquet"
)

table = pq.read_table(
    BytesIO(response["Body"].read())
)

df_minio = spark.createDataFrame(table.to_pandas())

print("Successfully loaded MinIO data into Spark!")
print("Rows:", df_minio.count())
print("Columns:", df_minio.columns)

display(df_minio)

# COMMAND ----------

from pyspark.sql.functions import col, when

df_silver = (
    df_minio
    .withColumn(
        "customer_type",
        when(col("balance") >= 3000, "VIP")
        .otherwise("REGULAR")
    )
)

display(df_silver)

# COMMAND ----------

import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Convert Spark DataFrame → Pandas → PyArrow
pdf_silver = df_silver.toPandas()
table_silver = pa.Table.from_pandas(pdf_silver)

# Write Parquet to memory
buffer = BytesIO()
pq.write_table(table_silver, buffer)
buffer.seek(0)

# Upload to MinIO
s3.put_object(
    Bucket="silver",
    Key="demo/customers_silver.parquet",
    Body=buffer.getvalue()
)

print("Silver data successfully written to MinIO!")
print("MinIO path: silver/demo/customers_silver.parquet")

# COMMAND ----------

response = s3.list_objects_v2(
    Bucket="silver",
    Prefix="demo/",
    MaxKeys=10
)

print("Objects in MinIO silver/demo/:")

for obj in response.get("Contents", []):
    print("-", obj["Key"], "| Size:", obj["Size"], "bytes")

# COMMAND ----------

import pyarrow.parquet as pq
from io import BytesIO

response = s3.get_object(
    Bucket="silver",
    Key="demo/customers_silver.parquet"
)

table = pq.read_table(
    BytesIO(response["Body"].read())
)

df_silver_check = spark.createDataFrame(table.to_pandas())

print("Successfully read Silver data from MinIO!")
print("Rows:", df_silver_check.count())
print("Columns:", df_silver_check.columns)

display(df_silver_check)
