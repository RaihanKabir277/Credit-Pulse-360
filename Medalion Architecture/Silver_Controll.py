# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
CATALOG = "creditpulse"
 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.control")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ### control.pipeline_tables — the table registry

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType
 
pipeline_tables_schema = StructType([
    StructField("table_name", StringType()),
    StructField("bronze_table", StringType()),
    StructField("silver_table", StringType()),
    StructField("quarantine_table", StringType()),
    StructField("primary_key_cols", ArrayType(StringType())),
    StructField("order_by_col_desc", StringType()),
    StructField("process_order", IntegerType()),
    StructField("is_active", BooleanType()),
])
 
pipeline_tables_data = [
    ("customers", f"{CATALOG}.bronze.customers", f"{CATALOG}.silver.customers",
     f"{CATALOG}.quarantine.customers", ["customer_id"], "_ingested_at", 1, True),
 
    ("loan_applications", f"{CATALOG}.bronze.loan_applications", f"{CATALOG}.silver.loan_applications",
     f"{CATALOG}.quarantine.loan_applications", ["application_id"], "_ingested_at", 2, True),
 
    ("loan_accounts", f"{CATALOG}.bronze.loan_accounts", f"{CATALOG}.silver.loan_accounts",
     f"{CATALOG}.quarantine.loan_accounts", ["loan_account_id"], "_ingested_at", 3, True),
 
    ("repayment_schedule", f"{CATALOG}.bronze.repayment_schedule", f"{CATALOG}.silver.repayment_schedule",
     f"{CATALOG}.quarantine.repayment_schedule", ["payment_id"], "_ingested_at", 4, True),
 
    ("credit_bureau_reports", f"{CATALOG}.bronze.credit_bureau_reports", f"{CATALOG}.silver.credit_bureau_reports",
     f"{CATALOG}.quarantine.credit_bureau_reports", ["report_id"], "_ingested_at", 4, True),
 
    ("collections_delinquency", f"{CATALOG}.bronze.collections_delinquency", f"{CATALOG}.silver.collections_delinquency",
     f"{CATALOG}.quarantine.collections_delinquency", ["case_id"], "_ingested_at", 5, True),
]
 
df_pipeline_tables = spark.createDataFrame(pipeline_tables_data, schema=pipeline_tables_schema)
df_pipeline_tables.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.control.pipeline_tables")
 
display(spark.table(f"{CATALOG}.control.pipeline_tables"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### control.column_type_map — per-column casting instructions

# COMMAND ----------

column_type_map_data = [
    # customers
    ("customers", "date_of_birth", "date"),
    ("customers", "onboarding_date", "date"),
    ("customers", "monthly_income_bdt", "decimal(15,2)"),
    ("customers", "credit_score", "int"),
 
    # loan_applications
    ("loan_applications", "application_date", "date"),
    ("loan_applications", "decision_date", "date"),
    ("loan_applications", "requested_amount_bdt", "decimal(15,2)"),
    ("loan_applications", "monthly_income_declared_bdt", "decimal(15,2)"),
    ("loan_applications", "existing_emi_obligations_bdt", "decimal(15,2)"),
    ("loan_applications", "debt_to_income_ratio", "decimal(8,4)"),
    ("loan_applications", "requested_tenure_months", "int"),
    ("loan_applications", "approved_amount_bdt", "decimal(15,2)"),
    ("loan_applications", "interest_rate_offered", "decimal(8,4)"),
 
    # loan_accounts
    ("loan_accounts", "disbursement_date", "date"),
    ("loan_accounts", "maturity_date", "date"),
    ("loan_accounts", "principal_amount_bdt", "decimal(15,2)"),
    ("loan_accounts", "interest_rate_annual", "decimal(8,4)"),
    ("loan_accounts", "tenure_months", "int"),
    ("loan_accounts", "emi_amount_bdt", "decimal(15,2)"),
    ("loan_accounts", "outstanding_principal_bdt", "decimal(15,2)"),
    ("loan_accounts", "collateral_value_bdt", "decimal(15,2)"),
 
    # repayment_schedule
    ("repayment_schedule", "due_date", "date"),
    ("repayment_schedule", "paid_date", "date"),
    ("repayment_schedule", "installment_number", "int"),
    ("repayment_schedule", "emi_due_amount_bdt", "decimal(15,2)"),
    ("repayment_schedule", "principal_component_bdt", "decimal(15,2)"),
    ("repayment_schedule", "interest_component_bdt", "decimal(15,2)"),
    ("repayment_schedule", "paid_amount_bdt", "decimal(15,2)"),
    ("repayment_schedule", "days_late", "int"),
 
    # credit_bureau_reports
    ("credit_bureau_reports", "report_date", "date"),
    ("credit_bureau_reports", "bureau_score", "int"),
    ("credit_bureau_reports", "total_active_loans", "int"),
    ("credit_bureau_reports", "total_outstanding_debt_bdt", "decimal(15,2)"),
    ("credit_bureau_reports", "credit_utilization_ratio", "decimal(8,4)"),
    ("credit_bureau_reports", "inquiries_last_6m", "int"),
    ("credit_bureau_reports", "delinquent_accounts_count", "int"),
    ("credit_bureau_reports", "oldest_credit_line_years", "decimal(6,1)"),
 
    # collections_delinquency
    ("collections_delinquency", "case_open_date", "date"),
    ("collections_delinquency", "last_contact_date", "date"),
    ("collections_delinquency", "promise_to_pay_date", "date"),
    ("collections_delinquency", "dpd_days", "int"),
    ("collections_delinquency", "overdue_amount_bdt", "decimal(15,2)"),
    ("collections_delinquency", "total_calls_made", "int"),
]
 
df_type_map = spark.createDataFrame(column_type_map_data, ["table_name", "column_name", "target_type"])
df_type_map.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.control.column_type_map")
 
print(f"column_type_map: {df_type_map.count()} rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### control.required_columns — not-null enforcement, per table

# COMMAND ----------

required_columns_data = [
    ("customers", "customer_id"), ("customers", "credit_score"), ("customers", "kyc_status"),
    ("loan_applications", "application_id"), ("loan_applications", "customer_id"), ("loan_applications", "application_status"),
    ("loan_accounts", "loan_account_id"), ("loan_accounts", "application_id"), ("loan_accounts", "principal_amount_bdt"),
    ("repayment_schedule", "payment_id"), ("repayment_schedule", "loan_account_id"), ("repayment_schedule", "due_date"),
    ("credit_bureau_reports", "report_id"), ("credit_bureau_reports", "customer_id"), ("credit_bureau_reports", "bureau_score"),
    ("collections_delinquency", "case_id"), ("collections_delinquency", "loan_account_id"), ("collections_delinquency", "dpd_days"),
]
 
df_required = spark.createDataFrame(required_columns_data, ["table_name", "column_name"])
df_required.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.control.required_columns")
 
print(f"required_columns: {df_required.count()} rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### control.standardize_columns — text normalization rules

# COMMAND ----------

standardize_columns_data = [
    ("customers", "kyc_status", "trim_lower"),
    ("customers", "employment_type", "trim_lower"),
    ("customers", "risk_segment", "trim_lower"),
    ("customers", "division", "trim"),
    ("loan_applications", "application_status", "trim_lower"),
    ("loan_applications", "loan_type", "trim_lower"),
    ("loan_accounts", "loan_status", "trim_lower"),
    ("loan_accounts", "loan_type", "trim_lower"),
    ("repayment_schedule", "payment_status", "trim_lower"),
    ("collections_delinquency", "collection_stage", "trim_lower"),
    ("collections_delinquency", "recovery_status", "trim_lower"),
]
 
df_standardize = spark.createDataFrame(standardize_columns_data, ["table_name", "column_name", "standardize_type"])
df_standardize.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.control.standardize_columns")
 
print(f"standardize_columns: {df_standardize.count()} rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### control.fk_rules — the FK graph, including one CONDITIONAL fk

# COMMAND ----------

fk_rules_data = [
    ("loan_applications", "customer_id", "customers", "customer_id", None, None),
    ("loan_accounts", "application_id", "loan_applications", "application_id", "application_status", "approved"),
    ("loan_accounts", "customer_id", "customers", "customer_id", None, None),
    ("repayment_schedule", "loan_account_id", "loan_accounts", "loan_account_id", None, None),
    ("repayment_schedule", "customer_id", "customers", "customer_id", None, None),
    ("credit_bureau_reports", "customer_id", "customers", "customer_id", None, None),
    ("collections_delinquency", "loan_account_id", "loan_accounts", "loan_account_id", None, None),
    ("collections_delinquency", "customer_id", "customers", "customer_id", None, None),
]
 
df_fk = spark.createDataFrame(
    fk_rules_data,
    ["table_name", "fk_column", "parent_table", "parent_key", "condition_column", "condition_value"],
)
df_fk.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.control.fk_rules")
 
print(f"fk_rules: {df_fk.count()} rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### control.value_rules — allowed values / numeric ranges, as JSON config

# COMMAND ----------

import json
 
value_rules_data = [
    ("customers", "credit_score", "numeric_range", json.dumps({"min": 300, "max": 900})),
    ("customers", "kyc_status", "allowed_values", json.dumps(["verified", "pending", "rejected"])),
    ("customers", "monthly_income_bdt", "not_negative", json.dumps({})),
    ("loan_applications", "application_status", "allowed_values",
     json.dumps(["approved", "rejected", "pending", "withdrawn"])),
    ("loan_applications", "debt_to_income_ratio", "numeric_range", json.dumps({"min": 0, "max": 5})),
    ("loan_accounts", "loan_status", "allowed_values",
     json.dumps(["active", "closed", "written_off", "foreclosed"])),
    ("loan_accounts", "principal_amount_bdt", "not_negative", json.dumps({})),
    ("repayment_schedule", "payment_status", "allowed_values",
     json.dumps(["pending", "paid_on_time", "paid_late", "missed"])),
    ("repayment_schedule", "days_late", "not_negative", json.dumps({})),
    ("credit_bureau_reports", "bureau_score", "numeric_range", json.dumps({"min": 300, "max": 900})),
    ("collections_delinquency", "dpd_days", "not_negative", json.dumps({})),
]
 
df_value_rules = spark.createDataFrame(
    value_rules_data, ["table_name", "column_name", "rule_type", "rule_param"]
)
df_value_rules.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.control.value_rules")
 
print(f"value_rules: {df_value_rules.count()} rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### control.watermark — one row per table, updated after each successful run

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.control.watermark (
    table_name STRING,
    last_processed_batch_id STRING,
    last_processed_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.control.pipeline_audit (
    run_id STRING,
    table_name STRING,
    run_started_at TIMESTAMP,
    run_ended_at TIMESTAMP,
    rows_read BIGINT,
    rows_valid BIGINT,
    rows_quarantined BIGINT,
    status STRING,
    message STRING
) USING DELTA
""")
 
print("Control layer setup complete.")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.control"))