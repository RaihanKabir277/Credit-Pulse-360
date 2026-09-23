# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import json
import uuid
from datetime import datetime, timezone
 
from pyspark.sql import functions as F
 
CATALOG = "creditpulse"
RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.now(timezone.utc)
 
pipeline_tables = spark.table(f"{CATALOG}.control.pipeline_tables").filter("is_active = true").collect()
fk_rules_df = spark.table(f"{CATALOG}.control.fk_rules")
gold_metrics = spark.table(f"{CATALOG}.control.gold_metrics").filter("is_active = true").collect()
 
AGG_FUNCS = {
    "sum": F.sum, "avg": F.avg, "count": F.count,
    "count_distinct": F.countDistinct, "min": F.min, "max": F.max,
}

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
 
AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("table_name", StringType()),
    StructField("run_started_at", TimestampType()),
    StructField("run_ended_at", TimestampType()),
    StructField("rows_read", LongType()),
    StructField("rows_valid", LongType()),
    StructField("rows_quarantined", LongType()),
    StructField("status", StringType()),
    StructField("message", StringType()),
])
 
 
def log_gold_audit(table_name, rows_valid, status, message=""):
    spark.createDataFrame(
        [(RUN_ID, table_name, RUN_STARTED_AT, datetime.now(timezone.utc), None, rows_valid, 0, status, message)],
        schema=AUDIT_SCHEMA,
    ).write.format("delta").mode("append").saveAsTable(f"{CATALOG}.control.pipeline_audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### join every configured parent, dynamically

# COMMAND ----------

def _silver_table_fqn(table_name: str) -> str:
    match = [t for t in pipeline_tables if t["table_name"] == table_name]
    if not match:
        raise ValueError(f"'{table_name}' not found in control.pipeline_tables")
    return match[0]["silver_table"]
 
 
def build_enriched_view(table_name: str):
    """Starts from a Silver table and left-joins every distinct parent
    relationship found in control.fk_rules for this table. Parent columns
    are prefixed with the parent's table_name to avoid collisions, and
    lineage columns (_source_file, _batch_id, etc.) are dropped before
    joining since they're not dashboard-relevant."""
    df = spark.table(_silver_table_fqn(table_name))
    df = df.select([c for c in df.columns if not c.startswith("_")])
 
    parent_links = (
        fk_rules_df.filter(F.col("table_name") == table_name)
        .select("fk_column", "parent_table", "parent_key")
        .distinct()
        .collect()
    )
 
    for link in parent_links:
        fk_col, parent_table, parent_key = link["fk_column"], link["parent_table"], link["parent_key"]
        if fk_col not in df.columns:
            continue
 
        parent_df = spark.table(_silver_table_fqn(parent_table))
        parent_df = parent_df.select([c for c in parent_df.columns if not c.startswith("_")])
 
        rename_map = {c: f"{parent_table}_{c}" for c in parent_df.columns if c != parent_key}
        parent_renamed = parent_df.select(
            [F.col(c).alias(rename_map.get(c, c)) for c in parent_df.columns]
        )
 
        df = df.join(parent_renamed, df[fk_col] == parent_renamed[parent_key], "left")
        df = df.drop(parent_renamed[parent_key])
 
    return df

# COMMAND ----------

results_summary = []
 
for cfg in gold_metrics:
    gold_table_name = cfg["gold_table_name"]
    print(f"\n=== Building {gold_table_name} ===")
 
    try:
        df = build_enriched_view(cfg["source_table"])
 
        group_cols = list(cfg["group_by_cols"])
        if cfg["date_bucket_col"]:
            df = df.withColumn(cfg["date_bucket_alias"], F.date_trunc("month", F.col(cfg["date_bucket_col"])))
            group_cols = group_cols + [cfg["date_bucket_alias"]]
 
        measures = json.loads(cfg["measures_json"])
        agg_exprs = [AGG_FUNCS[m["agg"]](F.col(m["column"])).alias(m["alias"]) for m in measures]
 
        result = df.groupBy(*group_cols).agg(*agg_exprs)
 
        target_table = f"{CATALOG}.gold.{gold_table_name}"
        (
            result.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
 
        row_count = result.count()
        results_summary.append((target_table, row_count))
        print(f"  {target_table}: {row_count:,} rows ({len(group_cols)} dimensions, {len(measures)} measures)")
        log_gold_audit(gold_table_name, row_count, "success")
 
    except Exception as e:
        print(f"  FAILED: {e}")
        log_gold_audit(gold_table_name, 0, "failed", str(e))
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run summary

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.gold"))
for table, count in results_summary:
    print(f"{table}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### upload to Minio

# COMMAND ----------

import boto3
from io import StringIO
 
MINIO_ENDPOINT = "************"
MINIO_ACCESS_KEY = "************"
MINIO_SECRET_KEY = "************"
 
CATALOG = "creditpulse"
GOLD_BUCKET = "gold"          
GOLD_PREFIX = "creditpulse"
 
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="************",
)
gold_tables = spark.table(f"{CATALOG}.control.gold_metrics").filter("is_active = true").collect()
 
for cfg in gold_tables:
    gold_table_name = cfg["gold_table_name"]
    target_table = f"{CATALOG}.gold.{gold_table_name}"
 
    pdf = spark.table(target_table).toPandas()
 
    buffer = StringIO()
    pdf.to_csv(buffer, index=False)
 
    key = f"{GOLD_PREFIX}/{gold_table_name}.csv"
    s3.put_object(Bucket=GOLD_BUCKET, Key=key, Body=buffer.getvalue().encode("utf-8"))
 
    print(f"Exported {target_table} -> s3://{GOLD_BUCKET}/{key} ({len(pdf):,} rows)")
 
print("\nAll Gold tables exported to MinIO. Databricks Gold tables remain the")
print("dashboard's live query source; MinIO holds the durable, rebuildable copy.")
