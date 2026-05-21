#!/usr/bin/env python3
# =============================================================================
# CAUTION: THIS SCRIPT ENABLES DATABRICKS LAKEHOUSE MONITORING ON EVERY
# NON-SYSTEM TABLE IN YOUR UNITY CATALOG WORKSPACE.
#
# - This will create monitors on potentially THOUSANDS of tables.
# - Each monitor runs on a schedule and consumes DBUs.
# - Enabling monitoring on very large or high-churn tables can significantly
#   increase your Databricks costs.
# - There is no bulk undo — you must disable monitors individually or run a
#   companion cleanup script.
#
# BEFORE RUNNING:
#   1. Run with --dry-run first to see exactly which tables will be affected.
#   2. Review the list carefully and use --exclude-catalogs / --exclude-schemas
#      to scope it down if needed.
#   3. Confirm your assets_dir and output_schema exist and the service principal
#      has CREATE TABLE on that schema.
#   4. Get explicit sign-off from the workspace admin.
# =============================================================================

import argparse
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries, MonitorCronSchedule


SYSTEM_CATALOGS = {"system", "hive_metastore", "__databricks_internal"}


def list_eligible_tables(w: WorkspaceClient, exclude_catalogs: set[str], exclude_schemas: set[str]) -> list[dict]:
    rows = w.statement_execution.execute_statement(
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        statement="""
            SELECT table_catalog, table_schema, table_name,
                   concat_ws('.', table_catalog, table_schema, table_name) AS fqn
            FROM system.information_schema.tables
            WHERE table_catalog NOT IN ({system})
              AND table_type = 'MANAGED'
            ORDER BY table_catalog, table_schema, table_name
        """.format(system=", ".join(f"'{c}'" for c in SYSTEM_CATALOGS)),
        wait_timeout="30s",
    )

    tables = []
    for row in (rows.result.data_array or []):
        catalog, schema, table, fqn = row
        if catalog in exclude_catalogs:
            continue
        if f"{catalog}.{schema}" in exclude_schemas:
            continue
        tables.append({"catalog": catalog, "schema": schema, "table": table, "fqn": fqn})
    return tables


def enable_monitor(w: WorkspaceClient, fqn: str, assets_dir: str, output_schema: str) -> str:
    try:
        w.quality_monitors.create(
            table_name=fqn,
            assets_dir=f"{assets_dir}/{fqn.replace('.', '/')}",
            output_schema_name=output_schema,
            time_series=MonitorTimeSeries(timestamp_col=""),  # set per-table if known
            schedule=MonitorCronSchedule(quartz_cron_expression="0 0 * * * ?"),  # hourly
        )
        return "created"
    except Exception as e:
        msg = str(e)
        if "already exists" in msg.lower() or "conflict" in msg.lower():
            return "already_exists"
        raise


def main():
    parser = argparse.ArgumentParser(description="Enable Databricks Lakehouse Monitoring on all non-system tables.")
    parser.add_argument("--assets-dir",      required=True, help="Workspace path for monitor assets, e.g. /Shared/dqm-monitors")
    parser.add_argument("--output-schema",   required=True, help="Schema to write monitor output tables, e.g. main.dqm_output")
    parser.add_argument("--exclude-catalogs", default="",  help="Comma-separated catalog names to skip")
    parser.add_argument("--exclude-schemas",  default="",  help="Comma-separated catalog.schema pairs to skip")
    parser.add_argument("--implement",         action="store_true", help="Actually enable monitoring; omit to do a dry run")
    parser.add_argument("--delay-ms",         type=int, default=200, help="Milliseconds to wait between API calls (default: 200)")
    args = parser.parse_args()

    exclude_catalogs = {c.strip() for c in args.exclude_catalogs.split(",") if c.strip()}
    exclude_schemas  = {s.strip() for s in args.exclude_schemas.split(",")  if s.strip()}

    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    print("Discovering eligible tables...")
    tables = list_eligible_tables(w, exclude_catalogs, exclude_schemas)
    print(f"Found {len(tables)} eligible tables.")

    if not args.implement:
        print("\nChanges will not be made, use --implement to make changes.")
        for t in tables:
            print(f"  {t['fqn']}")
        print(f"\nTotal: {len(tables)} tables would have monitoring enabled.")
        sys.exit(0)

    if not tables:
        print("No eligible tables found. Exiting.")
        sys.exit(0)

    print(f"\nEnabling monitoring on {len(tables)} tables. This may take a while...")
    print(f"Assets dir    : {args.assets_dir}")
    print(f"Output schema : {args.output_schema}")
    print()

    created = skipped = failed = 0
    failures = []

    for i, t in enumerate(tables, 1):
        fqn = t["fqn"]
        try:
            result = enable_monitor(w, fqn, args.assets_dir, args.output_schema)
            if result == "created":
                created += 1
                print(f"[{i}/{len(tables)}] ✓ {fqn}")
            else:
                skipped += 1
                print(f"[{i}/{len(tables)}] - {fqn} (already has monitor)")
        except Exception as e:
            failed += 1
            failures.append((fqn, str(e)))
            print(f"[{i}/{len(tables)}] ✗ {fqn} — {e}")

        time.sleep(args.delay_ms / 1000)

    print(f"\nDone. Created: {created}  Skipped: {skipped}  Failed: {failed}")

    if failures:
        print("\nFailed tables:")
        for fqn, err in failures:
            print(f"  {fqn}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()