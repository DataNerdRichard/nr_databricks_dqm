# Databricks Data Quality Monitoring — New Relic Integration

Runs data quality checks against Databricks tables and ships results to New Relic
as custom events for dashboarding and alerting.

> **Note:** This integration has been implemented and tested on **Azure** only. Compatibility with AWS and GCP Databricks deployments has not been verified. Key areas to confirm on other clouds: availability of `system.data_quality_monitoring`, Unity Catalog requirements, and secret scope configuration.

## Which notebook should I use?

| Notebook | Use when |
|---|---|
| `notebooks/dqm_query_simple.py` | You want to ship all DQM-enabled tables to New Relic with no filtering |
| `notebooks/dqm_query.py` | You need include/exclude glob patterns to control which tables are shipped |

Both notebooks use the same watermark mechanism, secrets, and job parameters (except `dqm_query.py` adds `include_patterns` and `exclude_patterns`). Start with the simple version unless you have a specific reason to filter.

## Prerequisites

### Enable DQM on your Databricks tables

The notebooks read from `system.data_quality_monitoring`, which only contains data for tables that have DQM explicitly enabled in Databricks. You must enable DQM on each table you want to monitor before running the notebook.

#### Azure (Unity Catalog)

- [Anomaly detection with Databricks DQM](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/) — how to enable and configure DQM on tables (Azure)
- [system.data_quality_monitoring table reference](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/data-quality-monitoring) — schema of the system table the notebooks query (Azure)
- TODO: Add equivalent DQM enablement and system table reference links for AWS
- TODO: Add equivalent DQM enablement and system table reference links for GCP

DQM on Azure requires Unity Catalog. To enable it on a table:

1. Navigate to the table in the Databricks catalog explorer.
2. Open the **Quality** tab.
3. Enable data quality monitoring and configure the check schedule.

Repeat for each table you want monitored. Once enabled, Databricks will begin writing results to `system.data_quality_monitoring` on the configured schedule.

To enable DQM programmatically across many tables, see `scripts/dqm_enable.py`.

## Local Development Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Add to `~/.zshrc`:
```bash
export NEW_RELIC_LICENSE_KEY=...NRAL
export NEW_RELIC_ACCOUNT_ID=1234567
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...
```

### Databricks secret scope

The notebooks read New Relic credentials from a Databricks secret scope at runtime — not from environment variables or job parameters. The default scope name is `nr-dqm` (configurable via the `secret_scope` job parameter).

Create the scope and store both secrets using the Databricks CLI:

```bash
databricks secrets create-scope nr-dqm
databricks secrets put-secret nr-dqm NEW_RELIC_API_KEY    --string-value "...NRAL"
databricks secrets put-secret nr-dqm NEW_RELIC_ACCOUNT_ID --string-value "1234567"
```

Or create the scope in the Databricks UI under **Settings > Secrets**, then add the two keys:

| Key | Value |
|---|---|
| `NEW_RELIC_API_KEY` | Your New Relic ingest/license key (ends in `NRAL`) |
| `NEW_RELIC_ACCOUNT_ID` | Your New Relic account ID |

The notebooks will raise a clear error at startup if either secret is missing or empty.

### Watermark table

The notebook persists a watermark to a Delta table (default: `main.default.dqm_nr_watermark`) so each run only queries new DQM rows. You can either let the notebook create it on first run, or create it in advance:

```sql
CREATE TABLE IF NOT EXISTS main.default.dqm_nr_watermark (watermark_ts TIMESTAMP);
```

### Service principal permissions

The job's service principal requires:

| Permission | Object | Notes |
|---|---|---|
| `SELECT` | `system.data_quality_monitoring` | Query DQM results |
| `SELECT` | `system.information_schema.tables` | Discover tables |
| `CREATE TABLE` | `main.default` (schema) | Only if not creating the watermark table in advance |
| `SELECT`, `MODIFY` | `main.default.dqm_nr_watermark` | Read and update watermark each run |

### Table include / exclude patterns

The job uses `fnmatch` glob patterns applied to fully-qualified table names (`catalog.schema.table`) to control which tables are monitored.

**`include_patterns`** — comma-separated list of patterns. A table must match at least one to be included. Defaults to `*.*.*` (all tables).

**`exclude_patterns`** — comma-separated list of patterns. A table matching any of these is excluded even if it matched an include pattern. Defaults to empty (nothing excluded).

Examples:

| Goal | include_patterns | exclude_patterns |
|---|---|---|
| All tables | `*.*.*` | _(blank)_ |
| One catalog only | `main.*.*` | _(blank)_ |
| Two schemas | `main.sales.*,main.finance.*` | _(blank)_ |
| Exclude staging | `main.*.*` | `*.staging.*,*.temp_*` |

Set these as job parameters in the Databricks job UI: **Job > Edit > Parameters**.

### Job scheduling

Set the job to run on your desired interval (e.g. every 20 minutes). On the first run there is no watermark, so the notebook falls back to `lookback_minutes_fallback` to determine how far back to query. **Set `lookback_minutes_fallback` to at least your run interval** to avoid gaps on that first cold-start run. Once the watermark is established it drives all subsequent queries and this setting is ignored.

## Project Structure

| Folder | Purpose |
|--------|---------|
| `dq_checks/` | Data quality check modules |
| `newrelic/` | New Relic Events API client |
| `notebooks/` | Databricks orchestration notebooks |
| `config/` | Check definitions and thresholds |
| `docs/` | Design docs and runbooks |
