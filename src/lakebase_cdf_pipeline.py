# Databricks notebook source

# MAGIC %md
# MAGIC # PostgreSQL CDF → Delta Lakehouse Pipeline
# MAGIC
# MAGIC A fully generic, parameterized PySpark pipeline that reads PostgreSQL /
# MAGIC Lakebase Change Data Feed (CDF) history tables and applies CDC merge logic
# MAGIC to produce clean, current-state Delta tables.
# MAGIC
# MAGIC **Point it at any Lakebase CDF schema and it builds the pipeline** — there
# MAGIC is no table-specific logic. Tables are discovered by pattern and each
# MAGIC table's primary key is resolved generically:
# MAGIC 1. UC-declared `PRIMARY KEY` constraint (authoritative), else
# MAGIC 2. the configurable default key column (`primary_key_col`, default `id`)
# MAGIC    if that column exists on the table, else
# MAGIC 3. the table is **skipped** and reported (no key → can't merge safely).
# MAGIC
# MAGIC **Other features:**
# MAGIC - Incremental or full processing via `_pg_lsn` watermarking (numeric)
# MAGIC - Automatic deduplication of CDC events per primary key
# MAGIC - Delta MERGE with insert, update, and delete handling (composite keys OK)
# MAGIC - Per-table error isolation — one table's failure won't stop the rest

# COMMAND ----------

# DBTITLE 1,Widget Parameters
# ---------------------------------------------------------------------------
# Widget Parameters - Full Pipeline Parameterization
# ---------------------------------------------------------------------------

# Source location
dbutils.widgets.text("source_catalog", "rd_classic_catalog", "Source Catalog")
dbutils.widgets.text("source_schema", "lakebase_benchmarks", "Source Schema")

# Target location
dbutils.widgets.text("target_catalog", "rd_classic_catalog", "Target Catalog")
dbutils.widgets.text("target_schema", "lakebase_benchmarks_current", "Target Schema")

# Table naming conventions
dbutils.widgets.text("table_prefix", "lb_s1tnt1_", "Table Prefix")
dbutils.widgets.text("table_suffix", "_history", "Table Suffix")

# CDC metadata column names
dbutils.widgets.text("pg_change_type_col", "_pg_change_type", "PG Change Type Column")
dbutils.widgets.text("pg_lsn_col", "_pg_lsn", "PG LSN Column")
dbutils.widgets.text("sort_by_col", "_sort_by", "Sort By Column")
dbutils.widgets.text("timestamp_col", "_timestamp", "Timestamp Column")

# Default primary key column, used ONLY when a table has no UC PRIMARY KEY
# constraint. This is a uniform convention, not per-table logic.
dbutils.widgets.text("primary_key_col", "id", "Default Primary Key Column")

# Processing options
dbutils.widgets.dropdown("processing_mode", "incremental", ["full", "incremental"], "Processing Mode")
dbutils.widgets.dropdown("enable_delete_handling", "true", ["true", "false"], "Enable Delete Handling")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"], "Log Level")

print("✓ All widgets created successfully")

# COMMAND ----------

# DBTITLE 1,Configuration
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

config = {
    "source_catalog": dbutils.widgets.get("source_catalog"),
    "source_schema": dbutils.widgets.get("source_schema"),
    "target_catalog": dbutils.widgets.get("target_catalog"),
    "target_schema": dbutils.widgets.get("target_schema"),
    "table_prefix": dbutils.widgets.get("table_prefix"),
    "table_suffix": dbutils.widgets.get("table_suffix"),
    "pg_change_type_col": dbutils.widgets.get("pg_change_type_col"),
    "pg_lsn_col": dbutils.widgets.get("pg_lsn_col"),
    "sort_by_col": dbutils.widgets.get("sort_by_col"),
    "timestamp_col": dbutils.widgets.get("timestamp_col"),
    "primary_key_col": dbutils.widgets.get("primary_key_col"),
    "processing_mode": dbutils.widgets.get("processing_mode"),
    "enable_delete_handling": dbutils.widgets.get("enable_delete_handling") == "true",
    "log_level": dbutils.widgets.get("log_level"),
}

# CDC metadata columns to exclude from target tables
CDC_META_COLS = [
    config["pg_change_type_col"], config["pg_lsn_col"], "_pg_xid",
    config["timestamp_col"], config["sort_by_col"],
]

print(f"✓ Configuration loaded | Mode: {config['processing_mode']} | Delete handling: {config['enable_delete_handling']}")
print(f"  Source: {config['source_catalog']}.{config['source_schema']} "
      f"(tables '{config['table_prefix']}*{config['table_suffix']}')")
print(f"  Target: {config['target_catalog']}.{config['target_schema']}")
print(f"  Default PK column (fallback): {config['primary_key_col']}")

# COMMAND ----------

# DBTITLE 1,Logging & Metrics Setup
# ---------------------------------------------------------------------------
# Logging & Metrics Setup
# ---------------------------------------------------------------------------
import logging
import time
from datetime import datetime
from collections import OrderedDict

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cdf_pipeline")
logger.setLevel(getattr(logging, config["log_level"]))


class PipelineMetrics:
    """Track pipeline execution metrics across all tables."""

    def __init__(self):
        self.start_time = time.time()
        self.tables_processed = 0
        self.tables_skipped = 0
        self.total_rows_inserted = 0
        self.total_rows_updated = 0
        self.total_rows_deleted = 0
        self.errors = []
        self.table_metrics = OrderedDict()

    def record_table(self, entity_name, rows_inserted=0, rows_updated=0, rows_deleted=0,
                     rows_source=0, duration_sec=0.0, status="success", error_msg=None):
        self.table_metrics[entity_name] = {
            "rows_source": rows_source,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_deleted": rows_deleted,
            "duration_sec": round(duration_sec, 2),
            "status": status,
            "error": error_msg,
        }
        if status == "success":
            self.tables_processed += 1
            self.total_rows_inserted += rows_inserted
            self.total_rows_updated += rows_updated
            self.total_rows_deleted += rows_deleted
        elif status == "skipped":
            self.tables_skipped += 1
        else:
            self.errors.append({"table": entity_name, "error": error_msg})

    def summary_df(self):
        rows = []
        for entity, m in self.table_metrics.items():
            rows.append((
                entity, m["rows_source"], m["rows_inserted"], m["rows_updated"],
                m["rows_deleted"], m["duration_sec"], m["status"], m.get("error", ""),
            ))
        schema = "entity STRING, rows_source LONG, rows_inserted LONG, rows_updated LONG, rows_deleted LONG, duration_sec DOUBLE, status STRING, error STRING"
        return spark.createDataFrame(rows, schema)

    def print_summary(self):
        elapsed = round(time.time() - self.start_time, 2)
        print("\n" + "=" * 70)
        print(f"PIPELINE SUMMARY  |  Elapsed: {elapsed}s")
        print(f"  Tables processed: {self.tables_processed}  |  Skipped: {self.tables_skipped}  |  Errors: {len(self.errors)}")
        print(f"  Rows inserted: {self.total_rows_inserted}  |  Updated: {self.total_rows_updated}  |  Deleted: {self.total_rows_deleted}")
        if self.errors:
            print("  ERRORS:")
            for e in self.errors:
                print(f"    - {e['table']}: {e['error']}")
        print("=" * 70)


metrics = PipelineMetrics()
print(f"✓ Logging configured (level: {config['log_level']}) | PipelineMetrics ready")

# COMMAND ----------

# DBTITLE 1,Helper Functions
# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


def get_source_tables(spark, source_catalog, source_schema, table_prefix, table_suffix):
    """Discover all matching CDF history tables from information_schema."""
    query = f"""
    SELECT table_name
    FROM {source_catalog}.information_schema.tables
    WHERE table_schema = '{source_schema}'
      AND table_name LIKE '{table_prefix}%{table_suffix}'
      AND table_type IN ('MANAGED', 'EXTERNAL', 'BASE TABLE', 'TABLE')
    ORDER BY table_name
    """
    rows = spark.sql(query).collect()
    tables = [row.table_name for row in rows]
    logger.info(f"Discovered {len(tables)} source tables matching '{table_prefix}*{table_suffix}'")
    return tables


def get_entity_name(table_name, table_prefix, table_suffix):
    """Strip prefix and suffix to derive entity name."""
    name = table_name
    if name.startswith(table_prefix):
        name = name[len(table_prefix):]
    if name.endswith(table_suffix):
        name = name[:-len(table_suffix)]
    return name


def get_primary_keys(spark, catalog, schema, table_name, default_pk_col):
    """Resolve a table's primary key GENERICALLY — no table-specific logic.

    Order of precedence:
      1. UC-declared PRIMARY KEY constraint (authoritative for Lakebase-managed
         tables) — supports composite keys, returned in ordinal order.
      2. The configurable default key column (`primary_key_col`) if it exists on
         the table — a uniform convention, applied to every table equally.
      3. [] — no key could be determined; caller should skip the table.

    Returns (primary_keys: list[str], source: str).
    """
    # 1. UC primary key constraint
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
    except Exception as e:
        logger.debug(f"Constraint lookup failed for {table_name}: {e}")
        pk_cols = []
    if pk_cols:
        return pk_cols, "uc_constraint"

    # 2. default key column, if present on the table
    cols = [f.name.lower() for f in spark.table(f"{catalog}.{schema}.{table_name}").schema.fields]
    if default_pk_col.lower() in cols:
        return [default_pk_col], "default_column"

    # 3. undeterminable
    return [], "none"


def get_latest_watermark(spark, target_table, lsn_col="_last_pg_lsn"):
    """Read the max _last_pg_lsn (LONG) from the target table; returns 0 if it
    does not exist or is empty. Kept numeric so the incremental filter is an
    exact LONG > LONG comparison (no string coercion / precision loss)."""
    try:
        result = spark.sql(f"SELECT MAX({lsn_col}) AS max_lsn FROM {target_table}").collect()
        val = result[0].max_lsn
        return int(val) if val is not None else 0
    except AnalysisException:
        logger.info(f"Target table {target_table} does not exist yet; watermark = 0")
        return 0


def get_table_columns(spark, source_table, cdc_meta_cols):
    """Return business columns (excluding CDC metadata columns)."""
    all_cols = [f.name for f in spark.table(source_table).schema.fields]
    return [c for c in all_cols if c.lower() not in [m.lower() for m in cdc_meta_cols]]


def ensure_target_schema(spark, target_catalog, target_schema):
    """Create the target schema if it doesn't exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    logger.info(f"Ensured target schema exists: {target_catalog}.{target_schema}")


def table_exists(spark, table_name):
    """Check if a table exists without throwing."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except AnalysisException:
        return False


print("✓ Helper functions defined")

# COMMAND ----------

# DBTITLE 1,Core CDC Merge Function
# ---------------------------------------------------------------------------
# Core CDC Merge Function
# ---------------------------------------------------------------------------
from delta.tables import DeltaTable
from pyspark.sql.window import Window


def apply_cdc_merge(
    spark, source_table, target_table, primary_keys, cdc_meta_cols,
    pg_change_type_col, pg_lsn_col, sort_by_col, timestamp_col,
    processing_mode, watermark, enable_delete_handling,
):
    """
    Apply CDC merge from a PostgreSQL CDF history table to a Delta target table.
    Returns dict with: rows_source, rows_inserted, rows_updated, rows_deleted
    """
    result = {"rows_source": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}

    # 1. Read source CDF history table
    source_df = spark.table(source_table)

    # 2. If incremental mode, filter where _pg_lsn > watermark.
    #    watermark is a LONG; F.lit(int) produces a numeric literal so this is an
    #    exact integer comparison (both sides LONG), robust under ANSI mode.
    if processing_mode == "incremental" and watermark and watermark > 0:
        source_df = source_df.filter(F.col(pg_lsn_col) > F.lit(watermark))
        logger.info(f"Incremental filter applied: {pg_lsn_col} > {watermark}")

    source_count = source_df.count()
    result["rows_source"] = source_count

    if source_count == 0:
        logger.info(f"No new CDC events for {source_table}; skipping.")
        return result

    logger.info(f"Processing {source_count} CDC events from {source_table}")

    # 3. Deduplicate: keep only the latest change per primary key
    pk_cols = primary_keys
    window_spec = Window.partitionBy(*[F.col(pk) for pk in pk_cols]).orderBy(F.col(sort_by_col).desc())
    deduped_df = (
        source_df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 4. Split into upserts and deletes
    # Support both full-word (Lakebase CDF) and single-letter (PG logical replication) types
    upsert_types = ["I", "U", "c", "u", "insert", "update"]
    delete_types = ["D", "d", "delete"]

    upserts_df = deduped_df.filter(F.col(pg_change_type_col).isin(upsert_types))
    deletes_df = deduped_df.filter(F.col(pg_change_type_col).isin(delete_types))

    # 5. Select business columns + audit columns
    business_cols = get_table_columns(spark, source_table, cdc_meta_cols)
    select_cols = [F.col(c) for c in business_cols]
    audit_cols = [
        F.col(pg_lsn_col).alias("_last_pg_lsn"),
        F.col(timestamp_col).alias("_last_cdc_timestamp"),
    ]

    upserts_final = upserts_df.select(*select_cols, *audit_cols)
    deletes_final = deletes_df.select(*select_cols, *audit_cols)

    # Combine with action marker for merge
    upserts_marked = upserts_final.withColumn("_cdc_action", F.lit("upsert"))
    deletes_marked = deletes_final.withColumn("_cdc_action", F.lit("delete"))
    merged_source = upserts_marked.unionByName(deletes_marked)

    upsert_count = upserts_final.count()
    delete_count = deletes_final.count()

    # 6. If target doesn't exist, create from upserts
    if not table_exists(spark, target_table):
        logger.info(f"Target table {target_table} does not exist; creating from upserts.")
        if upsert_count > 0:
            upserts_final.write.format("delta").mode("overwrite").saveAsTable(target_table)
            result["rows_inserted"] = upsert_count
            logger.info(f"Created {target_table} with {upsert_count} rows.")
        else:
            logger.warning(f"No upserts to create {target_table}; skipping table creation.")
        return result

    # 7. Delta MERGE for existing target
    delta_target = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

    merge_builder = delta_target.alias("target").merge(
        merged_source.alias("source"),
        merge_condition,
    )

    if enable_delete_handling and delete_count > 0:
        merge_builder = merge_builder.whenMatchedDelete(
            condition="source._cdc_action = 'delete'"
        )

    update_set = {c: f"source.{c}" for c in business_cols}
    update_set["_last_pg_lsn"] = "source._last_pg_lsn"
    update_set["_last_cdc_timestamp"] = "source._last_cdc_timestamp"

    merge_builder = merge_builder.whenMatchedUpdate(
        condition="source._cdc_action = 'upsert'",
        set=update_set,
    )

    insert_values = {c: f"source.{c}" for c in business_cols}
    insert_values["_last_pg_lsn"] = "source._last_pg_lsn"
    insert_values["_last_cdc_timestamp"] = "source._last_cdc_timestamp"

    merge_builder = merge_builder.whenNotMatchedInsert(
        condition="source._cdc_action = 'upsert'",
        values=insert_values,
    )

    merge_builder.execute()

    # Read merge metrics from Delta history
    history_df = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()
    op_metrics = history_df[0].operationMetrics if history_df else {}

    result["rows_inserted"] = int(op_metrics.get("numTargetRowsInserted", 0))
    result["rows_updated"] = int(op_metrics.get("numTargetRowsUpdated", 0))
    result["rows_deleted"] = int(op_metrics.get("numTargetRowsDeleted", 0))

    logger.info(
        f"Merge complete for {target_table}: "
        f"+{result['rows_inserted']} ins, ~{result['rows_updated']} upd, -{result['rows_deleted']} del"
    )
    return result


print("✓ Core CDC merge function defined")

# COMMAND ----------

# DBTITLE 1,Main Pipeline Orchestrator
# ---------------------------------------------------------------------------
# Main Pipeline Orchestrator
#
# Builds a plan by DISCOVERING every source table and resolving its primary key
# generically. No table names or per-table logic anywhere.
# ---------------------------------------------------------------------------

# Populated by build_plan(); reused by the summary cell.
PIPELINE_PLAN = []


def build_plan():
    """Discover source tables and resolve each one's primary key generically."""
    tables = get_source_tables(
        spark, config["source_catalog"], config["source_schema"],
        config["table_prefix"], config["table_suffix"],
    )
    plan = []
    for table_name in tables:
        entity_name = get_entity_name(table_name, config["table_prefix"], config["table_suffix"])
        pks, pk_source = get_primary_keys(
            spark, config["source_catalog"], config["source_schema"],
            table_name, config["primary_key_col"],
        )
        plan.append({
            "entity_name": entity_name,
            "source_table": f"{config['source_catalog']}.{config['source_schema']}.{table_name}",
            "target_table": f"{config['target_catalog']}.{config['target_schema']}.{entity_name}",
            "primary_keys": pks,
            "pk_source": pk_source,
        })
    return plan


def run_pipeline():
    """Orchestrate the full CDC merge pipeline across all discovered tables."""
    global PIPELINE_PLAN
    logger.info("=" * 70)
    logger.info("STARTING CDF → Delta Pipeline")
    logger.info(f"Mode: {config['processing_mode']} | Delete handling: {config['enable_delete_handling']}")
    logger.info("=" * 70)

    # 1. Ensure target schema exists
    ensure_target_schema(spark, config["target_catalog"], config["target_schema"])

    # 2. Discover source tables and resolve keys
    PIPELINE_PLAN = build_plan()
    logger.info(f"Planned {len(PIPELINE_PLAN)} discovered source tables")

    # 3. Process each discovered table
    for item in PIPELINE_PLAN:
        entity_name = item["entity_name"]
        source_table = item["source_table"]
        target_table = item["target_table"]
        primary_keys = item["primary_keys"]
        table_start = time.time()

        # Skip tables with no resolvable primary key — cannot merge safely.
        if not primary_keys:
            logger.warning(
                f"No primary key for {source_table} (no UC PRIMARY KEY constraint and "
                f"no '{config['primary_key_col']}' column); skipping {entity_name}. "
                f"Add a UC PRIMARY KEY constraint to include it."
            )
            metrics.record_table(
                entity_name, status="skipped",
                error_msg="No resolvable primary key",
                duration_sec=time.time() - table_start,
            )
            continue

        try:
            logger.info(f"Processing: {entity_name} ({source_table} → {target_table}) "
                        f"| PK {primary_keys} via {item['pk_source']}")

            watermark = 0
            if config["processing_mode"] == "incremental":
                watermark = get_latest_watermark(spark, target_table)
                logger.info(f"  Watermark for {entity_name}: {watermark}")

            result = apply_cdc_merge(
                spark=spark,
                source_table=source_table,
                target_table=target_table,
                primary_keys=primary_keys,
                cdc_meta_cols=CDC_META_COLS,
                pg_change_type_col=config["pg_change_type_col"],
                pg_lsn_col=config["pg_lsn_col"],
                sort_by_col=config["sort_by_col"],
                timestamp_col=config["timestamp_col"],
                processing_mode=config["processing_mode"],
                watermark=watermark,
                enable_delete_handling=config["enable_delete_handling"],
            )

            elapsed = time.time() - table_start

            if result["rows_source"] == 0:
                metrics.record_table(
                    entity_name, status="skipped",
                    error_msg="No new CDC events",
                    duration_sec=elapsed,
                )
            else:
                metrics.record_table(
                    entity_name,
                    rows_inserted=result["rows_inserted"],
                    rows_updated=result["rows_updated"],
                    rows_deleted=result["rows_deleted"],
                    rows_source=result["rows_source"],
                    duration_sec=elapsed,
                    status="success",
                )

        except Exception as e:
            elapsed = time.time() - table_start
            error_msg = str(e)[:500]
            logger.error(f"FAILED processing {entity_name}: {error_msg}")
            metrics.record_table(
                entity_name, status="error",
                error_msg=error_msg,
                duration_sec=elapsed,
            )
            continue

    # 4. Print summary
    metrics.print_summary()

    # 5. Return success/failure status
    if metrics.errors:
        logger.warning(f"Pipeline completed with {len(metrics.errors)} error(s)")
        return False
    logger.info("Pipeline completed successfully")
    return True


pipeline_success = run_pipeline()

# COMMAND ----------

# DBTITLE 1,Pipeline Summary & Validation
# ---------------------------------------------------------------------------
# Pipeline Summary & Validation
# ---------------------------------------------------------------------------

print("Per-Table Pipeline Metrics:")
display(metrics.summary_df())

comparison_rows = []
for item in PIPELINE_PLAN:
    source_table = item["source_table"]
    target_table = item["target_table"]
    primary_keys = item["primary_keys"]
    try:
        src_count = spark.table(source_table).count()
    except Exception:
        src_count = -1
    try:
        tgt_count = spark.table(target_table).count()
    except Exception:
        tgt_count = -1
    try:
        # distinct on the resolved primary key(s) — generic, no hardcoded column
        src_distinct_pks = spark.table(source_table).select(*primary_keys).distinct().count() if primary_keys else -1
    except Exception:
        src_distinct_pks = -1
    comparison_rows.append((
        item["entity_name"], source_table, target_table,
        ",".join(primary_keys) if primary_keys else "(none)",
        src_count, src_distinct_pks, tgt_count,
    ))

comparison_schema = "entity STRING, source_table STRING, target_table STRING, primary_keys STRING, source_total_rows LONG, source_distinct_pks LONG, target_rows LONG"
comparison_df = spark.createDataFrame(comparison_rows, comparison_schema)

print("\nSource vs Target Row Count Comparison:")
display(comparison_df)

if pipeline_success:
    print("\n✓ Pipeline completed successfully!")
    dbutils.notebook.exit("SUCCESS")
else:
    print("\n⚠ Pipeline completed with errors - check metrics above.")
    # Raise so the job RUN is marked FAILED, which triggers max_retries and
    # on_failure notifications. NOTE: dbutils.notebook.exit() always marks the
    # run SUCCESS regardless of the string, so it cannot signal failure.
    raise RuntimeError(
        f"Pipeline completed with {len(metrics.errors)} table error(s): "
        + ", ".join(e["table"] for e in metrics.errors)
    )
