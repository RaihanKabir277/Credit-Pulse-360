# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import uuid
from datetime import datetime, timezone
from io import BytesIO
 
import boto3
import pandas as pd
from pyspark.sql import functions as F
 
MINIO_ENDPOINT = "************"
MINIO_ACCESS_KEY = "************"
MINIO_SECRET_KEY = "************"
 
LANDING_BUCKET = "creditpulse"       
LANDING_PREFIX = "data"              
RAW_ARCHIVE_BUCKET = "bronze"       
ARCHIVE_PREFIX = "creditpulse"       
CATALOG = "creditpulse"
SCHEMA = "bronze"
 
TABLES = {
    "customers": "customers.csv",
    "loan_applications": "loan_applications.csv",
    "loan_accounts": "loan_accounts.csv",
    "repayment_schedule": "repayment_schedule.csv",
    "credit_bureau_reports": "credit_bureau_reports.csv",
    "collections_delinquency": "collections_delinquency.csv",
}
 
BATCH_ID = str(uuid.uuid4())
INGESTED_AT = datetime.now(timezone.utc).isoformat()
 
print(f"Batch ID: {BATCH_ID}")
print(f"Ingested at: {INGESTED_AT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — MinIO client (boto3)

# COMMAND ----------

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the catalog/schema if they don't already exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Catalog/schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Archive each raw CSV unmodified into the `bronze` MinIO bucket

# COMMAND ----------

for table_name, file_name in TABLES.items():
    origin_key = f"{LANDING_PREFIX}/{file_name}"
    archive_key = f"{ARCHIVE_PREFIX}/{file_name}"
    s3.copy_object(
        Bucket=RAW_ARCHIVE_BUCKET,
        CopySource={"Bucket": LANDING_BUCKET, "Key": origin_key},
        Key=archive_key,
    )
    print(f"Archived {LANDING_BUCKET}/{origin_key} -> {RAW_ARCHIVE_BUCKET}/{archive_key}")
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read each archived CSV via boto3 -> pandas -> Spark, then write Delta

# COMMAND ----------


load_summary = []
 
for table_name, file_name in TABLES.items():
    origin_key = f"{LANDING_PREFIX}/{file_name}"
    archive_key = f"{ARCHIVE_PREFIX}/{file_name}"
 
    response = s3.get_object(Bucket=RAW_ARCHIVE_BUCKET, Key=archive_key)
    pdf = pd.read_csv(BytesIO(response["Body"].read()))
 
    df = (
        spark.createDataFrame(pdf)
        .withColumn("_origin_bucket", F.lit(LANDING_BUCKET))
        .withColumn("_origin_key", F.lit(origin_key))
        .withColumn("_archive_bucket", F.lit(RAW_ARCHIVE_BUCKET))
        .withColumn("_archive_key", F.lit(archive_key))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingested_at", F.lit(INGESTED_AT).cast("timestamp"))
    )
 
    target_table = f"{CATALOG}.{SCHEMA}.{table_name}"
 
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
 
    row_count = df.count()
    col_count = len(df.columns)
    load_summary.append((target_table, row_count, col_count))
    print(f"{target_table}: {row_count:,} rows, {col_count} columns (incl. 6 lineage columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify every table actually landed 

# COMMAND ----------

for target_table, expected_row_count, _ in load_summary:
    actual_row_count = spark.table(target_table).count()
    assert actual_row_count > 0, f"Bronze load produced 0 rows for {target_table} — stopping."
    assert actual_row_count == expected_row_count, (
        f"{target_table}: row count changed between write and verification "
        f"({expected_row_count:,} -> {actual_row_count:,})"
    )
    print(f"Verified {target_table}: {actual_row_count:,} rows")
 
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------


display(
    spark.table(f"{CATALOG}.{SCHEMA}.customers")
    .select("_origin_bucket", "_origin_key", "_archive_bucket", "_archive_key")
    .distinct()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM creditpulse.bronze.customers
