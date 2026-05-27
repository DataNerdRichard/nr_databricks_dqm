# Databricks notebook source
# Queries system.data_quality_monitoring.table_results for all DQM-enabled tables since the last
# watermark and ships results to New Relic. Simpler alternative to dqm_query.py —
# no include/exclude filtering; ships everything DQM has data for.
# Designed to run as a Databricks Serverless Job on Azure every 20 minutes.
#
# Job parameters (set in the job UI or API):
#   watermark_table          — fully-qualified Delta table to persist the watermark
#                              (default: main.default.dqm_nr_watermark)
#   lookback_minutes_fallback — minutes to look back on first run (no watermark yet);
#                              should be >= job run interval (default: 60)
#   secret_scope             — Databricks secret scope name (default: nr-dqm)
#
# Watermark behaviour:
#   - If the watermark table exists, queries DQM for event_time > last watermark.
#   - If the watermark table is absent (first run), falls back to now - lookback_minutes_fallback.
#   - Watermark is updated only after results are successfully sent to New Relic.
#
# Required secrets in the Databricks secret scope:
#   NEW_RELIC_API_KEY    — New Relic ingest/license key
#   NEW_RELIC_ACCOUNT_ID — Target New Relic account ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# These blocks can be deleted — they exist only to suppress IDE warnings.
# spark, dbutils, and pyspark are injected automatically by the Databricks runtime.
try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    spark: SparkSession
except ImportError:
    pass

try:
    dbutils = dbutils  # type: ignore[name-defined]
except NameError:
    dbutils = None  # type: ignore[assignment]

# COMMAND ----------

dbutils.widgets.text("watermark_table",           "main.default.dqm_nr_watermark", "Watermark Delta table (catalog.schema.table)")
dbutils.widgets.text("lookback_minutes_fallback", "",                               "Fallback lookback in minutes if no watermark (blank = use spark conf or 60)")
dbutils.widgets.text("secret_scope",              "nr-dqm",                        "Databricks secret scope")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import requests
from datetime import datetime, timedelta, timezone

watermark_table = dbutils.widgets.get("watermark_table").strip()
secret_scope    = dbutils.widgets.get("secret_scope").strip()

# Resolve lookback_minutes: widget → spark conf → hardcoded 60.
# To set via spark conf, add  dqm.lookback_minutes_fallback = <N>  to the job
# cluster's Spark config (Job > Edit > Advanced > Spark config).
_widget_minutes = dbutils.widgets.get("lookback_minutes_fallback").strip()
if _widget_minutes:
    lookback_minutes = int(_widget_minutes)
else:
    _conf_minutes = spark.conf.get("dqm.lookback_minutes_fallback", "")
    lookback_minutes = int(_conf_minutes) if _conf_minutes else 60

nr_api_key    = dbutils.secrets.get(scope=secret_scope, key="NEW_RELIC_API_KEY")
nr_account_id = dbutils.secrets.get(scope=secret_scope, key="NEW_RELIC_ACCOUNT_ID")

_missing = [k for k, v in {"NEW_RELIC_API_KEY": nr_api_key, "NEW_RELIC_ACCOUNT_ID": nr_account_id}.items() if not v]
if _missing:
    raise ValueError(f"Missing or empty secrets in scope '{secret_scope}': {', '.join(_missing)}. See README for secret scope setup.")

print(f"Watermark table   : {watermark_table}")
print(f"Lookback minutes  : {lookback_minutes}")
print(f"Secret scope      : {secret_scope}")
print(f"NR account ID     : {nr_account_id}")

NR_EVENTS_ENDPOINT = f"https://insights-collector.newrelic.com/v1/accounts/{nr_account_id}/events"

_NR_MAX_BATCH_BYTES = 900_000  # stay comfortably under the 1MB NR limit


def build_dqm_events(row: dict) -> list[dict]:
    base = {
        "event_time"    : int(row["event_time"].timestamp() * 1000) if row.get("event_time") else None,
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


def send_nr_events(events: list[dict]) -> int:
    """Send a list of NR events, chunking into <1MB batches. Returns total events sent."""
    batch, batch_bytes, total_sent = [], 2, 0  # 2 for outer []
    headers = {"Content-Type": "application/json", "Api-Key": nr_api_key}

    def flush(b):
        resp = requests.post(NR_EVENTS_ENDPOINT, data=json.dumps(b), headers=headers, timeout=30)
        resp.raise_for_status()

    for event in events:
        chunk = json.dumps(event)
        chunk_bytes = len(chunk.encode()) + 1  # +1 for comma
        if batch and batch_bytes + chunk_bytes > _NR_MAX_BATCH_BYTES:
            flush(batch)
            total_sent += len(batch)
            batch, batch_bytes = [], 2
        batch.append(event)
        batch_bytes += chunk_bytes

    if batch:
        flush(batch)
        total_sent += len(batch)

    return total_sent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark

# COMMAND ----------

watermark_exists = spark.catalog.tableExists(watermark_table)

if watermark_exists:
    since_ts = spark.sql(f"SELECT MAX(watermark_ts) AS wm FROM {watermark_table}") \
                    .collect()[0]["wm"]
    print(f"Watermark found  : {since_ts}")
else:
    since_ts = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    print(f"No watermark — falling back to {lookback_minutes} min ago: {since_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query DQM Since Watermark

# COMMAND ----------

import pyspark.sql.functions as F

raw_df = spark.sql(f"""
    SELECT
        *,
        concat_ws('.', catalog_name, schema_name, table_name) AS fqn
    FROM system.data_quality_monitoring.table_results
    WHERE event_time > '{since_ts}'
    ORDER BY event_time DESC
""")

dqm_count = raw_df.count()
print(f"New DQM rows since watermark: {dqm_count}")

raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send DQM Rows to New Relic

# COMMAND ----------

dqm_events = []
for row in raw_df.collect():
    dqm_events.extend(build_dqm_events(row.asDict(recursive=True)))

sent = send_nr_events(dqm_events)
print(f"Sent {sent} DQM events to New Relic.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send Job Run Summary to New Relic

# COMMAND ----------

send_nr_events([{
    "eventType"          : "DatabricksDQMJobRun",
    "dqm_rows_new"       : dqm_count,
    "watermark_used"     : str(since_ts),
    "watermark_was_cold" : not watermark_exists,
}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Table

# COMMAND ----------

summary_df = (
    raw_df
    .groupBy("fqn")
    .agg(
        F.count("*").alias("check_count"),
        F.min("event_time").alias("earliest_run"),
        F.max("event_time").alias("latest_run"),
    )
    .orderBy("fqn")
)

summary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Watermark

# COMMAND ----------

if dqm_count > 0:
    new_watermark = raw_df.agg(F.max("event_time").alias("max_event_time")).collect()[0]["max_event_time"]

    spark.createDataFrame([(new_watermark,)], ["watermark_ts"]) \
         .write.format("delta") \
         .mode("overwrite") \
         .option("overwriteSchema", "true") \
         .saveAsTable(watermark_table)

    print(f"Watermark updated: {since_ts} → {new_watermark}")
else:
    print("No new DQM rows — watermark unchanged.")

# COMMAND ----------

dbutils.notebook.exit(f"ok:{dqm_count}_new_rows")