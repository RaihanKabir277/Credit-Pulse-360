# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
CATALOG = "creditpulse"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### control gold metrics

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
 
gold_metrics_schema = StructType([
    StructField("gold_table_name", StringType()),
    StructField("source_table", StringType()),
    StructField("group_by_cols", ArrayType(StringType())),
    StructField("date_bucket_col", StringType()),
    StructField("date_bucket_alias", StringType()),
    StructField("measures_json", StringType()),
    StructField("is_active", BooleanType()),
])
 
def measures(*items):
    """items: (column, agg, alias) tuples -> JSON string for measures_json"""
    return json.dumps([{"column": c, "agg": a, "alias": al} for c, a, al in items])
 
gold_metrics_data = [
    (
        "loan_portfolio_summary", "loan_accounts",
        ["loan_type", "loan_status", "customers_division", "customers_risk_segment"],
        "disbursement_date", "disbursement_month",
        measures(
            ("principal_amount_bdt", "sum", "total_principal_disbursed_bdt"),
            ("outstanding_principal_bdt", "sum", "total_outstanding_bdt"),
            ("loan_account_id", "count", "loan_count"),
            ("interest_rate_annual", "avg", "avg_interest_rate"),
            ("emi_amount_bdt", "avg", "avg_emi_bdt"),
        ),
        True,
    ),
    (
        "application_funnel", "loan_applications",
        ["loan_type", "application_status", "application_channel", "customers_division"],
        "application_date", "application_month",
        measures(
            ("application_id", "count", "application_count"),
            ("requested_amount_bdt", "sum", "total_requested_bdt"),
            ("approved_amount_bdt", "sum", "total_approved_bdt"),
            ("debt_to_income_ratio", "avg", "avg_dti_ratio"),
        ),
        True,
    ),
    (
        "repayment_performance", "repayment_schedule",
        ["payment_status", "customers_risk_segment", "loan_accounts_loan_type"],
        "due_date", "due_month",
        measures(
            ("payment_id", "count", "installment_count"),
            ("emi_due_amount_bdt", "sum", "total_emi_due_bdt"),
            ("paid_amount_bdt", "sum", "total_paid_bdt"),
            ("days_late", "avg", "avg_days_late"),
        ),
        True,
    ),
    (
        "collections_summary", "collections_delinquency",
        ["dpd_bucket", "collection_stage", "recovery_status", "customers_risk_segment"],
        "case_open_date", "case_open_month",
        measures(
            ("case_id", "count", "case_count"),
            ("overdue_amount_bdt", "sum", "total_overdue_bdt"),
            ("total_calls_made", "avg", "avg_calls_made"),
        ),
        True,
    ),
    (
        "credit_risk_distribution", "customers",
        ["risk_segment", "division", "employment_type", "kyc_status"],
        None, None,
        measures(
            ("customer_id", "count", "customer_count"),
            ("credit_score", "avg", "avg_credit_score"),
            ("monthly_income_bdt", "avg", "avg_monthly_income_bdt"),
        ),
        True,
    ),
    (
        "bureau_score_trend", "credit_bureau_reports",
        ["customers_risk_segment"],
        "report_date", "report_month",
        measures(
            ("report_id", "count", "report_count"),
            ("bureau_score", "avg", "avg_bureau_score"),
            ("credit_utilization_ratio", "avg", "avg_credit_utilization"),
            ("delinquent_accounts_count", "sum", "total_delinquent_accounts"),
        ),
        True,
    ),
]
 
df_gold_metrics = spark.createDataFrame(gold_metrics_data, schema=gold_metrics_schema)
df_gold_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.control.gold_metrics")
 
display(spark.table(f"{CATALOG}.control.gold_metrics"))
print(f"\n{df_gold_metrics.count()} Gold dashboard datasets configured.")