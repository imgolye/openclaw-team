#!/usr/bin/env python3
"""Export a legacy OpenClaw Team SQLite store into PostgreSQL-friendly files."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

NULL_MARKER = "__MISSION_CONTROL_PG_NULL__"

MANAGED_TABLES = (
    "metadata",
    "product_users",
    "audit_events",
    "product_installations",
    "management_runs",
    "automation_rules",
    "notification_channels",
    "automation_alerts",
    "notification_deliveries",
    "orchestration_workflows",
    "routing_policies",
    "tenants",
    "tenant_installations",
    "tenant_api_keys",
)


def sqlite_path(openclaw_dir):
    return Path(openclaw_dir).expanduser().resolve() / "dashboard" / "dashboard.db"


def pg_type(sqlite_type):
    normalized = str(sqlite_type or "").upper()
    if "INT" in normalized:
        return "BIGINT"
    if "REAL" in normalized or "FLOA" in normalized or "DOUB" in normalized:
        return "DOUBLE PRECISION"
    if "BLOB" in normalized:
        return "BYTEA"
    return "TEXT"


def quote_ident(value):
    return '"' + str(value).replace('"', '""') + '"'


def export_table(conn, table_name, output_dir):
    rows = conn.execute(f"PRAGMA table_info({quote_ident(table_name)})").fetchall()
    if not rows:
        return "", None

    columns = [row[1] for row in rows]
    pk_columns = [row[1] for row in rows if row[5]]
    schema_lines = [f"CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} ("]
    column_defs = []
    for row in rows:
        name = row[1]
        column_type = pg_type(row[2])
        not_null = " NOT NULL" if row[3] else ""
        column_defs.append(f"  {quote_ident(name)} {column_type}{not_null}")
    if pk_columns:
        quoted_pk = ", ".join(quote_ident(item) for item in pk_columns)
        column_defs.append(f"  PRIMARY KEY ({quoted_pk})")
    schema_lines.append(",\n".join(column_defs))
    schema_lines.append(");")

    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_path = data_dir / f"{table_name}.csv"
    query = f"SELECT {', '.join(quote_ident(column) for column in columns)} FROM {quote_ident(table_name)}"
    records = conn.execute(query).fetchall()
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for record in records:
            writer.writerow([NULL_MARKER if value is None else value for value in record])
    copy_line = (
        f"\\copy {quote_ident(table_name)} ({', '.join(quote_ident(column) for column in columns)}) "
        f"FROM 'data/{table_name}.csv' CSV HEADER NULL '{NULL_MARKER}';"
    )
    return "\n".join(schema_lines), copy_line


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="~/.openclaw", help="OpenClaw state dir")
    parser.add_argument("--output", default="./dist/postgres-export", help="Export directory")
    args = parser.parse_args()

    db_path = sqlite_path(args.dir)
    if not db_path.exists():
        raise SystemExit(f"Missing legacy SQLite store: {db_path}")

    output_dir = Path(args.output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        schema_parts = []
        copy_parts = []
        for table_name in MANAGED_TABLES:
            schema_sql, copy_sql = export_table(conn, table_name, output_dir)
            if schema_sql:
                schema_parts.append(schema_sql)
            if copy_sql:
                copy_parts.append(copy_sql)
        (output_dir / "schema.sql").write_text("\n\n".join(schema_parts) + "\n", encoding="utf-8")
        (output_dir / "load.sql").write_text("\n".join(copy_parts) + "\n", encoding="utf-8")
    finally:
        conn.close()

    print(f"Exported PostgreSQL bundle to {output_dir}")


if __name__ == "__main__":
    main()
