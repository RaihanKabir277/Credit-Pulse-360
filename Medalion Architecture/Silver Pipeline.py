# Databricks notebook source
# MAGIC %md
# MAGIC ### CreditPulse - Silver Layer: Generic Processing Engine

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone
 
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
 
CATALOG = "creditpulse"
RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.now(timezone.utc)
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load all config once per run

# COMMAND ----------

pipeline_tables = spark.table(f"{CATALOG}.control.pipeline_tables").filter("is_active = true") \
    .orderBy("process_order").collect()
type_map_df = spark.table(f"{CATALOG}.control.column_type_map")
required_cols_df = spark.table(f"{CATALOG}.control.required_columns")
standardize_df = spark.table(f"{CATALOG}.control.standardize_columns")
fk_rules_df = spark.table(f"{CATALOG}.control.fk_rules")
value_rules_df = spark.table(f"{CATALOG}.control.value_rules")
 
print(f"Loaded config for {len(pipeline_tables)} active tables, run_id={RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reusable engine functions - table-agnostic

# COMMAND ----------

def get_watermark(table_name: str):
    row = spark.table(f"{CATALOG}.control.watermark").filter(F.col("table_name") == table_name).collect()
    return row[0]["last_processed_batch_id"] if row else None
 
 
def update_watermark(table_name: str, batch_id: str):
    now = datetime.now(timezone.utc)
    new_row = spark.createDataFrame([(table_name, batch_id, now)],
                                     ["table_name", "last_processed_batch_id", "last_processed_at"])
    target = DeltaTable.forName(spark, f"{CATALOG}.control.watermark")
    (target.alias("t")
     .merge(new_row.alias("s"), "t.table_name = s.table_name")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
 
 
def read_bronze(bronze_table: str) -> DataFrame:
    """Reads the full Bronze snapshot. Once Bronze moves to append mode,
    change this one function to filter on _batch_id > watermark — every
    caller downstream is unaffected."""
    return spark.table(bronze_table)
 
 
def apply_type_casts(df: DataFrame, table_name: str) -> DataFrame:
    rules = type_map_df.filter(F.col("table_name") == table_name).collect()
    for r in rules:
        if r["column_name"] in df.columns:
            df = df.withColumn(r["column_name"], F.col(r["column_name"]).cast(r["target_type"]))
    return df
 
 
def apply_standardization(df: DataFrame, table_name: str) -> DataFrame:
    rules = standardize_df.filter(F.col("table_name") == table_name).collect()
    for r in rules:
        col, kind = r["column_name"], r["standardize_type"]
        if col not in df.columns:
            continue
        if kind == "trim_lower":
            df = df.withColumn(col, F.lower(F.trim(F.col(col))))
        elif kind == "trim_upper":
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))
        elif kind == "trim":
            df = df.withColumn(col, F.trim(F.col(col)))
    return df
 
 
def dedup(df: DataFrame, pk_cols: list, order_col: str) -> DataFrame:
    from pyspark.sql.window import Window
    w = Window.partitionBy(*pk_cols).orderBy(F.col(order_col).desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))
 
 
def validate_not_null(df: DataFrame, table_name: str):
    """Returns (valid_df, rejects_df_with_reason)."""
    required = [r["column_name"] for r in required_cols_df.filter(F.col("table_name") == table_name).collect()]
    required = [c for c in required if c in df.columns]
    if not required:
        return df, df.limit(0).withColumn("_dq_failure_reason", F.lit(None).cast("string"))
 
    null_condition = None
    for c in required:
        cond = F.col(c).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)
 
    rejects = df.filter(null_condition).withColumn(
        "_dq_failure_reason",
        F.lit("required_column_null: " + ",".join(required))
    )
    valid = df.filter(~null_condition)
    return valid, rejects
 
 
def validate_value_rules(df: DataFrame, table_name: str):
    """Returns (valid_df, rejects_df_with_reason). Evaluates allowed_values,
    numeric_range, and not_negative rules generically from JSON config."""
    rules = value_rules_df.filter(F.col("table_name") == table_name).collect()
    fail_condition = None
    reasons = []
 
    for r in rules:
        col, rule_type, param = r["column_name"], r["rule_type"], r["rule_param"]
        if col not in df.columns:
            continue
        param_json = json.loads(param) if param else {}
 
        if rule_type == "allowed_values":
            cond = ~F.col(col).isin(param_json) & F.col(col).isNotNull()
        elif rule_type == "numeric_range":
            cond = (F.col(col) < param_json.get("min")) | (F.col(col) > param_json.get("max"))
        elif rule_type == "not_negative":
            cond = F.col(col) < 0
        else:
            continue
 
        fail_condition = cond if fail_condition is None else (fail_condition | cond)
        reasons.append(f"{rule_type}:{col}")
 
    if fail_condition is None:
        return df, df.limit(0).withColumn("_dq_failure_reason", F.lit(None).cast("string"))
 
    rejects = df.filter(fail_condition).withColumn(
        "_dq_failure_reason", F.lit("value_rule_failed: " + ",".join(reasons))
    )
    valid = df.filter(~fail_condition)
    return valid, rejects
 
 
def validate_fk(df: DataFrame, table_name: str):
    """Returns (valid_df, rejects_df_with_reason). Supports conditional FKs
    where the parent row must also match condition_column = condition_value."""
    rules = fk_rules_df.filter(F.col("table_name") == table_name).collect()
    valid = df
    all_rejects = []
 
    for r in rules:
        fk_col, parent_table, parent_key = r["fk_column"], r["parent_table"], r["parent_key"]
        cond_col, cond_val = r["condition_column"], r["condition_value"]
 
        if fk_col not in valid.columns:
            continue
 
        parent_silver_row = [pt for pt in pipeline_tables if pt["table_name"] == parent_table]
        parent_table_fqn = parent_silver_row[0]["silver_table"] if parent_silver_row else f"{CATALOG}.silver.{parent_table}"
 
        try:
            parent_df = spark.table(parent_table_fqn)
        except AnalysisException:
            # Parent not yet processed this run (shouldn't happen if process_order is respected)
            continue
 
        if cond_col and cond_val is not None:
            parent_keys = parent_df.filter(F.col(cond_col) == cond_val).select(F.col(parent_key).alias("_pk"))
        else:
            parent_keys = parent_df.select(F.col(parent_key).alias("_pk"))
 
        joined = valid.join(parent_keys, valid[fk_col] == parent_keys["_pk"], "left")
        rejects = joined.filter(F.col("_pk").isNull()).drop("_pk").withColumn(
            "_dq_failure_reason", F.lit(f"fk_violation:{fk_col}->{parent_table}.{parent_key}")
        )
        valid = joined.filter(F.col("_pk").isNotNull()).drop("_pk")
        all_rejects.append(rejects)
 
    if all_rejects:
        rejects_union = all_rejects[0]
        for r in all_rejects[1:]:
            rejects_union = rejects_union.unionByName(r, allowMissingColumns=True)
        return valid, rejects_union
    return valid, df.limit(0).withColumn("_dq_failure_reason", F.lit(None).cast("string"))
 
 
def merge_into_silver(df: DataFrame, target_table: str, pk_cols: list):
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").saveAsTable(target_table)
        return
    target = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])
    (target.alias("t")
     .merge(df.alias("s"), merge_condition)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
 
 
def write_quarantine(df: DataFrame, target_table: str):
    df_tagged = df.withColumn("_quarantined_at", F.current_timestamp()).withColumn("_run_id", F.lit(RUN_ID))
    if not spark.catalog.tableExists(target_table):
        df_tagged.write.format("delta").saveAsTable(target_table)
    else:
        df_tagged.write.format("delta").mode("append").saveAsTable(target_table)
 
 
def log_audit(table_name, rows_read, rows_valid, rows_quarantined, status, message=""):
    row = spark.createDataFrame(
        [(RUN_ID, table_name, RUN_STARTED_AT, datetime.now(timezone.utc),
          rows_read, rows_valid, rows_quarantined, status, message)],
        ["run_id", "table_name", "run_started_at", "run_ended_at",
         "rows_read", "rows_valid", "rows_quarantined", "status", "message"],
    )
    row.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.control.pipeline_audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main loop - every table processed by the same code path

# COMMAND ----------

for cfg in pipeline_tables:
    table_name = cfg["table_name"]
    print(f"\n=== Processing {table_name} ===")
 
    try:
        bronze_df = read_bronze(cfg["bronze_table"])
        rows_read = bronze_df.count()
 
        current_batch_id = bronze_df.select("_batch_id").first()
        current_batch_id = current_batch_id["_batch_id"] if current_batch_id else None
 
        last_batch_id = get_watermark(table_name)
        if current_batch_id is not None and current_batch_id == last_batch_id:
            print(f"  Skipping — batch {current_batch_id} already processed (idempotent no-op).")
            log_audit(table_name, rows_read, 0, 0, "skipped_unchanged")
            continue
 
        df = apply_type_casts(bronze_df, table_name)
        df = apply_standardization(df, table_name)
        df = dedup(df, cfg["primary_key_cols"], cfg["order_by_col_desc"])
 
        df, reject_nulls = validate_not_null(df, table_name)
        df, reject_values = validate_value_rules(df, table_name)
        df, reject_fk = validate_fk(df, table_name)
 
        all_rejects = reject_nulls.unionByName(reject_values, allowMissingColumns=True) \
                                   .unionByName(reject_fk, allowMissingColumns=True)
        rows_quarantined = all_rejects.count()
        rows_valid = df.count()
 
        merge_into_silver(df, cfg["silver_table"], cfg["primary_key_cols"])
 
        if rows_quarantined > 0:
            write_quarantine(all_rejects, cfg["quarantine_table"])
 
        if current_batch_id is not None:
            update_watermark(table_name, current_batch_id)
 
        print(f"  Read: {rows_read:,} | Valid -> Silver: {rows_valid:,} | Quarantined: {rows_quarantined:,}")
        log_audit(table_name, rows_read, rows_valid, rows_quarantined, "success")
 
    except Exception as e:
        print(f"  FAILED: {e}")
        log_audit(table_name, 0, 0, 0, "failed", str(e))
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamically computed as-of date (no hardcoded date anywhere downstream)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.reference_dates AS
SELECT MAX(due_date) AS as_of_date, current_timestamp() AS computed_at
FROM {CATALOG}.silver.repayment_schedule
""")
 
display(spark.table(f"{CATALOG}.silver.reference_dates"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run summary

# COMMAND ----------

display(spark.table(f"{CATALOG}.control.pipeline_audit").filter(F.col("run_id") == RUN_ID))


# COMMAND ----------

import boto3
from io import StringIO
 
MINIO_ENDPOINT = "http://103.95.211.33:9000"
MINIO_ACCESS_KEY = "databricks"
MINIO_SECRET_KEY = "Databricks@123"
 
CATALOG = "creditpulse"
SILVER_BUCKET = "silver"       
SILVER_PREFIX = "creditpulse"
 
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)
 
pipeline_tables = spark.table(f"{CATALOG}.control.pipeline_tables").filter("is_active = true").collect()
 
for cfg in pipeline_tables:
    table_name = cfg["table_name"]
    silver_table = cfg["silver_table"]
 
    pdf = spark.table(silver_table).toPandas()
 
    buffer = StringIO()
    pdf.to_csv(buffer, index=False)
 
    key = f"{SILVER_PREFIX}/{table_name}.csv"
    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=buffer.getvalue().encode("utf-8"))
 
    print(f"Exported {silver_table} -> s3://{SILVER_BUCKET}/{key} ({len(pdf):,} rows)")
 
print("\nAll Silver tables exported to MinIO. This is now the durable system of record.")