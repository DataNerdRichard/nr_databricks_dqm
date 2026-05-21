# Databricks notebook source
# Exploration notebook — fetch raw DQM data and inspect struct shapes.
# No watermark, no New Relic, no side effects. Safe to run repeatedly.

# COMMAND ----------

# These blocks can be deleted — they exist only to suppress IDE warnings.
# spark, dbutils, and pyspark are injected automatically by the Databricks runtime.
try:
    from pyspark.sql import SparkSession
    spark: SparkSession
except ImportError:
    pass

try:
    dbutils = dbutils  # type: ignore[name-defined]
except NameError:
    dbutils = None  # type: ignore[assignment]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Sample Rows

# COMMAND ----------

raw_df = spark.sql("""
    SELECT *
    FROM system.data_quality_monitoring.table_results
    LIMIT 10
""")

raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Struct Fields

# COMMAND ----------

raw_df.select("freshness", "completeness", "downstream_impact", "root_cause_analysis").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Schema

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dump First Row as Python Dict

# COMMAND ----------

import json

row = raw_df.limit(1).collect()
if row:
    print(json.dumps(row[0].asDict(recursive=True), indent=2, default=str))
else:
    print("No rows returned.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview NR Event Payload (as would be sent)

# COMMAND ----------

def build_dqm_events(row: dict) -> list[dict]:
    base = {
        "event_time"    : str(row.get("event_time")),
        "catalog_name"  : row.get("catalog_name"),
        "schema_name"   : row.get("schema_name"),
        "table_name"    : row.get("table_name"),
        "table_id"      : row.get("table_id"),
        "overall_status": row.get("status"),
    }

    events = []

    f = row.get("freshness")
    if f:
        cf = f.get("commit_freshness") or {}
        events.append({
            "eventType"                        : "Databricks_DQM_Freshness",
            "freshness_status"                 : f.get("status"),
            "commit_freshness_status"          : cf.get("status"),
            "commit_freshness_last_value"      : str(cf["last_value"]) if cf.get("last_value") else None,
            "commit_freshness_predicted_value" : str(cf["predicted_value"]) if cf.get("predicted_value") else None,
            **base,
        })

    c = row.get("completeness")
    if c:
        trc = c.get("total_row_count") or {}
        drc = c.get("daily_row_count") or {}
        events.append({
            "eventType"                     : "Databricks_DQM_Completeness",
            "completeness_status"           : c.get("status"),
            "total_row_count_status"        : trc.get("status"),
            "total_row_count_last_value"    : trc.get("last_value"),
            "total_row_count_min_predicted" : trc.get("min_predicted_value"),
            "total_row_count_max_predicted" : trc.get("max_predicted_value"),
            "daily_row_count_status"        : drc.get("status"),
            "daily_row_count_last_value"    : drc.get("last_value"),
            "daily_row_count_min_predicted" : drc.get("min_predicted_value"),
            "daily_row_count_max_predicted" : drc.get("max_predicted_value"),
            **base,
        })

    d = row.get("downstream_impact")
    if d:
        events.append({
            "eventType"                     : "Databricks_DQM_DownstreamImpact",
            "impact_level"                  : d.get("impact_level"),
            "num_downstream_tables"         : d.get("num_downstream_tables"),
            "num_queries_on_affected_tables": d.get("num_queries_on_affected_tables"),
            **base,
        })

    return events

sample_events = []
for row in raw_df.collect():
    sample_events.extend(build_dqm_events(row.asDict(recursive=True)))

print(json.dumps(sample_events, indent=2, default=str))
