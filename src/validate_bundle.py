# Databricks notebook source

# MAGIC %md
# MAGIC # Lakebase CDF Pipeline — Native Pre-Flight Validation
# MAGIC
# MAGIC A Databricks-native validation job. It checks the **live workspace
# MAGIC preconditions** the CDF pipeline depends on and **fails the run** (raises)
# MAGIC on any hard problem, so it works as a native CI gate you can run before a
# MAGIC deploy or on a schedule. It contains **no table-specific logic** — point it
# MAGIC at any Lakebase CDF schema.
# MAGIC
# MAGIC It validates:
# MAGIC 1. Source catalog / schema are reachable
# MAGIC 2. At least one `{prefix}*{suffix}` CDF history table is discoverable
# MAGIC 3. Each discovered source table honors the **CDC schema contract**
# MAGIC    (`_pg_change_type` string; `_pg_lsn`/`_sort_by` integral; `_timestamp`
# MAGIC    a timestamp) — a violation is a **FAIL**
# MAGIC 4. Each table has a **resolvable primary key** (UC PRIMARY KEY constraint,
# MAGIC    else the default key column) — an unkeyable table is a **WARN** (the
# MAGIC    pipeline skips it), not a failure
# MAGIC 5. Target catalog is reachable
# MAGIC
# MAGIC Runs entirely on serverless via Spark SQL — no CLI, no shell, no extra deps.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("source_catalog", "rd_classic_catalog", "Source Catalog")
dbutils.widgets.text("source_schema", "lakebase_benchmarks", "Source Schema")
dbutils.widgets.text("target_catalog", "rd_classic_catalog", "Target Catalog")
dbutils.widgets.text("target_schema", "lakebase_benchmarks_current", "Target Schema")
dbutils.widgets.text("table_prefix", "lb_s1tnt1_", "Table Prefix")
dbutils.widgets.text("table_suffix", "_history", "Table Suffix")

# CDC schema contract (must match the pipeline's fixed column mappings)
dbutils.widgets.text("pg_change_type_col", "_pg_change_type", "PG Change Type Column")
dbutils.widgets.text("pg_lsn_col", "_pg_lsn", "PG LSN Column")
dbutils.widgets.text("sort_by_col", "_sort_by", "Sort By Column")
dbutils.widgets.text("timestamp_col", "_timestamp", "Timestamp Column")

# Default key column, used only when a table has no UC PRIMARY KEY constraint.
dbutils.widgets.text("primary_key_col", "id", "Default Primary Key Column")

cfg = {k: dbutils.widgets.get(k) for k in [
    "source_catalog", "source_schema", "target_catalog", "target_schema",
    "table_prefix", "table_suffix",
    "pg_change_type_col", "pg_lsn_col", "sort_by_col", "timestamp_col",
    "primary_key_col",
]}

print("Validating config:")
for k, v in cfg.items():
    print(f"  {k} = {v}")

# COMMAND ----------

# DBTITLE 1,Validation Framework
from pyspark.sql.types import (
    LongType, IntegerType, ShortType, ByteType,
    StringType, VarcharType, CharType,
    TimestampType, TimestampNTZType,
)

INTEGRAL_TYPES = (LongType, IntegerType, ShortType, ByteType)
STRING_TYPES = (StringType, VarcharType, CharType)
TIMESTAMP_TYPES = (TimestampType, TimestampNTZType)

results = []  # (check, status, detail) — status in {PASS, WARN, FAIL}


def record(check, status, detail=""):
    results.append((check, status, detail))
    marker = {"PASS": "✓", "WARN": "!", "FAIL": "✗"}.get(status, "?")
    print(f"  {marker} [{status}] {check}" + (f" — {detail}" if detail else ""))


def schema_exists(catalog, schema):
    q = (f"SELECT 1 FROM {catalog}.information_schema.schemata "
         f"WHERE schema_name = '{schema}' LIMIT 1")
    return len(spark.sql(q).collect()) > 0


def resolve_primary_keys(catalog, schema, table_name, default_pk_col):
    """Generic PK resolution: UC constraint, else default column if present."""
    q = f"""
    SELECT kcu.column_name
    FROM {catalog}.information_schema.table_constraints tc
    JOIN {catalog}.information_schema.key_column_usage kcu
      ON  tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema  = kcu.constraint_schema
      AND tc.constraint_name    = kcu.constraint_name
    WHERE tc.table_schema = '{schema}'
      AND tc.table_name   = '{table_name}'
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY kcu.ordinal_position
    """
    try:
        pk_cols = [r.column_name for r in spark.sql(q).collect()]
    except Exception:
        pk_cols = []
    if pk_cols:
        return pk_cols, "uc_constraint"
    cols = [f.name.lower() for f in spark.table(f"{catalog}.{schema}.{table_name}").schema.fields]
    if default_pk_col.lower() in cols:
        return [default_pk_col], "default_column"
    return [], "none"


print("✓ Validation framework ready")

# COMMAND ----------

# DBTITLE 1,Run Checks
# --- 1. Source catalog / schema reachable -----------------------------------
source_ok = False
try:
    source_ok = schema_exists(cfg["source_catalog"], cfg["source_schema"])
    record(f"Source schema reachable: {cfg['source_catalog']}.{cfg['source_schema']}",
           "PASS" if source_ok else "FAIL",
           "" if source_ok else "schema not found in information_schema.schemata")
except Exception as e:
    record(f"Source schema reachable: {cfg['source_catalog']}.{cfg['source_schema']}",
           "FAIL", str(e)[:300])

# --- 2. Discover source CDF history tables ----------------------------------
discovered = []
if source_ok:
    try:
        pattern = f"{cfg['table_prefix']}%{cfg['table_suffix']}"
        q = (f"SELECT table_name FROM {cfg['source_catalog']}.information_schema.tables "
             f"WHERE table_schema = '{cfg['source_schema']}' "
             f"AND table_name LIKE '{pattern}' ORDER BY table_name")
        discovered = [r.table_name for r in spark.sql(q).collect()]
        record(f"Discover source tables matching '{pattern}'",
               "PASS" if discovered else "FAIL",
               f"found {len(discovered)}" if discovered else "no matching tables found")
    except Exception as e:
        record("Discover source tables", "FAIL", str(e)[:300])

# --- 3 & 4. Per-table CDC contract (FAIL) + primary key resolution (WARN) ----
required_cols = {
    cfg["pg_change_type_col"]: STRING_TYPES,
    cfg["pg_lsn_col"]: INTEGRAL_TYPES,
    cfg["sort_by_col"]: INTEGRAL_TYPES,
    cfg["timestamp_col"]: TIMESTAMP_TYPES,
}

for tbl in discovered:
    fq = f"{cfg['source_catalog']}.{cfg['source_schema']}.{tbl}"
    # CDC schema contract
    try:
        fields = {f.name.lower(): f.dataType for f in spark.table(fq).schema.fields}
        problems = []
        for col, expected in required_cols.items():
            dt = fields.get(col.lower())
            if dt is None:
                problems.append(f"missing '{col}'")
            elif not isinstance(dt, expected):
                exp = "/".join(t.__name__.replace("Type", "") for t in expected)
                problems.append(f"'{col}' is {type(dt).__name__.replace('Type','')}, expected {exp}")
        record(f"CDC contract: {tbl}", "PASS" if not problems else "FAIL", "; ".join(problems))
    except Exception as e:
        record(f"CDC contract: {tbl}", "FAIL", str(e)[:300])

    # Primary key resolution (unkeyable = WARN, pipeline will skip it)
    try:
        pks, pk_source = resolve_primary_keys(
            cfg["source_catalog"], cfg["source_schema"], tbl, cfg["primary_key_col"])
        if pks:
            record(f"Primary key: {tbl}", "PASS", f"{pks} via {pk_source}")
        else:
            record(f"Primary key: {tbl}", "WARN",
                   f"no UC PRIMARY KEY and no '{cfg['primary_key_col']}' column — "
                   f"pipeline will SKIP this table (add a UC PRIMARY KEY to include it)")
    except Exception as e:
        record(f"Primary key: {tbl}", "FAIL", str(e)[:300])

# --- 5. Target catalog reachable --------------------------------------------
try:
    spark.sql(f"SELECT 1 FROM {cfg['target_catalog']}.information_schema.schemata LIMIT 1").collect()
    tgt_schema_present = schema_exists(cfg["target_catalog"], cfg["target_schema"])
    record(f"Target catalog reachable: {cfg['target_catalog']}", "PASS",
           (f"target schema '{cfg['target_schema']}' exists"
            if tgt_schema_present
            else f"target schema '{cfg['target_schema']}' absent — pipeline will CREATE it "
                 f"(requires CREATE SCHEMA privilege)"))
except Exception as e:
    record(f"Target catalog reachable: {cfg['target_catalog']}", "FAIL", str(e)[:300])

# COMMAND ----------

# DBTITLE 1,Summary & Exit
schema_str = "check STRING, status STRING, detail STRING"
display(spark.createDataFrame(results, schema_str))

failed = [r for r in results if r[1] == "FAIL"]
warned = [r for r in results if r[1] == "WARN"]
passed = [r for r in results if r[1] == "PASS"]
print("\n" + "=" * 70)
print(f"VALIDATION SUMMARY  |  {len(passed)} passed, {len(warned)} warned, {len(failed)} failed")
print("=" * 70)
for check, _, detail in warned:
    print(f"  ! WARN {check}: {detail}")
for check, _, detail in failed:
    print(f"  ✗ FAIL {check}: {detail}")

if failed:
    # Raise so the job RUN is marked FAILED — this is the CI gate.
    # NOTE: dbutils.notebook.exit() always marks the run SUCCESS regardless of
    # the string, so it cannot signal failure; an exception is required.
    raise RuntimeError(
        f"{len(failed)} validation check(s) failed: "
        + "; ".join(check for check, _, _ in failed)
    )

msg = "SUCCESS" + (f" ({len(warned)} warning(s) — some tables will be skipped)" if warned else "")
print(f"\n✓ Validation passed. {msg}")
dbutils.notebook.exit(msg)
