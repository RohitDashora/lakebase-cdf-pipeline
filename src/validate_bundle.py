# Databricks notebook source

# MAGIC %md
# MAGIC # Lakebase CDF Pipeline — Native Pre-Flight Validation
# MAGIC
# MAGIC A Databricks-native validation job. It checks the **live workspace
# MAGIC preconditions** the CDF pipeline depends on and **fails the run** (non-zero
# MAGIC exit) on any problem, so it works as a native CI gate you can run before a
# MAGIC deploy or on a schedule.
# MAGIC
# MAGIC It validates:
# MAGIC 1. Source catalog / schema are reachable
# MAGIC 2. At least one `{prefix}*{suffix}` CDF history table is discoverable
# MAGIC 3. Each discovered source table honors the **CDC schema contract**:
# MAGIC    - `_pg_change_type` is a string
# MAGIC    - `_pg_lsn` and `_sort_by` are **integral** (LONG/INT) — required for the
# MAGIC      numeric watermark comparison
# MAGIC    - `_timestamp` is a timestamp
# MAGIC    - the primary-key column is present
# MAGIC 4. Target catalog is reachable (and reports whether the target schema exists)
# MAGIC
# MAGIC Runs entirely on serverless via Spark SQL — no CLI, no shell, no extra deps.

# COMMAND ----------

# DBTITLE 1,Parameters
# Wired from the same bundle variables as the pipeline so validation checks the
# exact config a deploy would use.
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
dbutils.widgets.text("primary_key_col", "id", "Primary Key Column")

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

results = []  # (check, status, detail)


def record(check, ok, detail=""):
    results.append((check, "PASS" if ok else "FAIL", detail))
    marker = "✓" if ok else "✗"
    print(f"  {marker} [{('PASS' if ok else 'FAIL')}] {check}"
          + (f" — {detail}" if detail else ""))
    return ok


def schema_exists(catalog, schema):
    q = (f"SELECT 1 FROM {catalog}.information_schema.schemata "
         f"WHERE schema_name = '{schema}' LIMIT 1")
    return len(spark.sql(q).collect()) > 0


print("✓ Validation framework ready")

# COMMAND ----------

# DBTITLE 1,Run Checks
# --- 1. Source catalog / schema reachable -----------------------------------
source_ok = False
try:
    source_ok = schema_exists(cfg["source_catalog"], cfg["source_schema"])
    record(f"Source schema reachable: {cfg['source_catalog']}.{cfg['source_schema']}",
           source_ok,
           "" if source_ok else "schema not found in information_schema.schemata")
except Exception as e:
    record(f"Source schema reachable: {cfg['source_catalog']}.{cfg['source_schema']}",
           False, str(e)[:300])

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
               len(discovered) > 0,
               f"found {len(discovered)}" if discovered else "no matching tables found")
    except Exception as e:
        record("Discover source tables", False, str(e)[:300])

# --- 3. CDC schema contract per discovered table ----------------------------
required_cols = {
    cfg["pg_change_type_col"]: STRING_TYPES,
    cfg["pg_lsn_col"]: INTEGRAL_TYPES,
    cfg["sort_by_col"]: INTEGRAL_TYPES,
    cfg["timestamp_col"]: TIMESTAMP_TYPES,
    cfg["primary_key_col"]: None,  # presence only
}

for tbl in discovered:
    fq = f"{cfg['source_catalog']}.{cfg['source_schema']}.{tbl}"
    try:
        fields = {f.name.lower(): f.dataType for f in spark.table(fq).schema.fields}
        problems = []
        for col, expected in required_cols.items():
            dt = fields.get(col.lower())
            if dt is None:
                problems.append(f"missing '{col}'")
            elif expected is not None and not isinstance(dt, expected):
                exp = "/".join(t.__name__.replace("Type", "") for t in expected)
                problems.append(f"'{col}' is {type(dt).__name__.replace('Type','')}, expected {exp}")
        record(f"CDC contract: {tbl}", not problems, "; ".join(problems))
    except Exception as e:
        record(f"CDC contract: {tbl}", False, str(e)[:300])

# --- 4. Target catalog reachable --------------------------------------------
try:
    spark.sql(f"SELECT 1 FROM {cfg['target_catalog']}.information_schema.schemata LIMIT 1").collect()
    tgt_schema_present = schema_exists(cfg["target_catalog"], cfg["target_schema"])
    record(f"Target catalog reachable: {cfg['target_catalog']}", True,
           (f"target schema '{cfg['target_schema']}' exists"
            if tgt_schema_present
            else f"target schema '{cfg['target_schema']}' absent — pipeline will CREATE it "
                 f"(requires CREATE SCHEMA privilege)"))
except Exception as e:
    record(f"Target catalog reachable: {cfg['target_catalog']}", False, str(e)[:300])

# COMMAND ----------

# DBTITLE 1,Summary & Exit
schema_str = "check STRING, status STRING, detail STRING"
display(spark.createDataFrame(results, schema_str))

failed = [r for r in results if r[1] == "FAIL"]
print("\n" + "=" * 70)
print(f"VALIDATION SUMMARY  |  {len(results) - len(failed)} passed, {len(failed)} failed")
print("=" * 70)

if failed:
    for check, _, detail in failed:
        print(f"  ✗ {check}: {detail}")
    # Raise so the job RUN is marked FAILED — this is the CI gate.
    # NOTE: dbutils.notebook.exit() always marks the run SUCCESS regardless of
    # the string, so it cannot signal failure; an exception is required.
    raise RuntimeError(
        f"{len(failed)} validation check(s) failed: "
        + "; ".join(check for check, _, _ in failed)
    )

print("✓ All validation checks passed.")
dbutils.notebook.exit("SUCCESS")
