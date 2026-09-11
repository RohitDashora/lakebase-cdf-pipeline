# Databricks notebook source

# MAGIC %md
# MAGIC # PostgreSQL CDF → Delta Lakehouse Pipeline
# MAGIC
# MAGIC A fully parameterized PySpark pipeline that reads PostgreSQL Change Data Feed (CDF)
# MAGIC history tables and applies CDC merge logic to produce clean, current-state Delta tables.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Incremental or full processing modes via `_pg_lsn` watermarking
# MAGIC - Automatic deduplication of CDC events per primary key
# MAGIC - Delta MERGE with insert, update, and delete handling
# MAGIC - Per-table error isolation — one table's failure won't stop the rest
# MAGIC - Configurable via Databricks widgets / job parameters
# MAGIC - Dynamic table discovery from information_schema

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
dbutils.widgets.text("primary_key_col", "id", "Primary Key Column")

# Processing options
dbutils.widgets.dropdown("processing_mode", "incremental", ["full", "incremental"], "Processing Mode")
dbutils.widgets.text("batch_size", "10000", "Batch Size")
dbutils.widgets.dropdown("enable_delete_handling", "true", ["true", "false"], "Enable Delete Handling")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"], "Log Level")

print("✓ All widgets created successfully")

# COMMAND ----------

# DBTITLE 1,Configuration & Table Registry
# ---------------------------------------------------------------------------
# Configuration & Table Registry
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
    "batch_size": int(dbutils.widgets.get("batch_size")),
    "enable_delete_handling": dbutils.widgets.get("enable_delete_handling") == "true",
    "log_level": dbutils.widgets.get("log_level"),
}

# CDC metadata columns to exclude from target tables
CDC_META_COLS = ["_pg_change_type", "_pg_lsn", "_pg_xid", "_timestamp", "_sort_by"]

# ---------------------------------------------------------------------------
# Table Registry: entity_name -> metadata
#
# This registry is auto-populated from discovered source tables.
# Override or extend it here to add custom primary keys or skip entities.
# ---------------------------------------------------------------------------
TABLE_REGISTRY = {
    "profile":              {"primary_keys": ["id"], "unique_business_key": "profileid"},
    "contactpointaddress":  {"primary_keys": ["id"], "unique_business_key": "contactpointid"},
    "contactpointemail":    {"primary_keys": ["id"], "unique_business_key": "contactpointid"},
    "contactpointphone":    {"primary_keys": ["id"], "unique_business_key": "contactpointid"},
    "contactpointsocial":   {"primary_keys": ["id"], "unique_business_key": "contactpointsocialid"},
    "education":            {"primary_keys": ["id"], "unique_business_key": "educationid"},
    "interest":             {"primary_keys": ["id"], "unique_business_key": "interestid"},
    "preference":           {"primary_keys": ["id"], "unique_business_key": "preferenceid"},
    "subscription":         {"primary_keys": ["id"], "unique_business_key": "subscriptionid"},
    "alternatekey":         {"primary_keys": ["id"], "unique_business_key": "alternatekeyid"},
}

# Build fully qualified table names
for entity_name, meta in TABLE_REGISTRY.items():
    src = f"{config['source_catalog']}.{config['source_schema']}.{config['table_prefix']}{entity_name}{config['table_suffix']}"
    tgt = f"{config['target_catalog']}.{config['target_schema']}.{entity_name}"
    meta["source_table"] = src
    meta["target_table"] = tgt

print(f"✓ Configuration loaded | Mode: {config['processing_mode']} | Delete handling: {config['enable_delete_handling']}")
print(f"✓ Table Registry: {len(TABLE_REGISTRY)} entities registered")
for name, meta in TABLE_REGISTRY.items():
    print(f"  {name}: {meta['source_table']} → {meta['target_table']}")

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


def get_latest_watermark(spark, target_table, lsn_col="_last_pg_lsn"):
    """Read the max _last_pg_lsn from the target table; returns '0' if not exists."""
    try:
        result = spark.sql(f"SELECT MAX({lsn_col}) AS max_lsn FROM {target_table}").collect()
        val = result[0].max_lsn
        return str(val) if val is not None else "0"
    except AnalysisException:
        logger.info(f"Target table {target_table} does not exist yet; watermark = '0'")
        return "0"


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

    # 2. If incremental mode, filter where _pg_lsn > watermark
    if processing_mode == "incremental" and watermark != "0":
        source_df = source_df.filter(F.col(pg_lsn_col) > F.lit(watermark))
        logger.info(f"Incremental filter applied: {pg_lsn_col} > '{watermark}'")

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
# ---------------------------------------------------------------------------


def run_pipeline():
    """Orchestrate the full CDC merge pipeline across all registered tables."""
    logger.info("=" * 70)
    logger.info("STARTING CDF → Delta Pipeline")
    logger.info(f"Mode: {config['processing_mode']} | Delete handling: {config['enable_delete_handling']}")
    logger.info("=" * 70)

    # 1. Ensure target schema exists
    ensure_target_schema(spark, config["target_catalog"], config["target_schema"])

    # 2. Discover source tables
    discovered_tables = get_source_tables(
        spark,
        config["source_catalog"],
        config["source_schema"],
        config["table_prefix"],
        config["table_suffix"],
    )
    logger.info(f"Discovered {len(discovered_tables)} source tables")

    # 3. Process each registered table
    for entity_name, meta in TABLE_REGISTRY.items():
        table_start = time.time()
        source_table = meta["source_table"]
        target_table = meta["target_table"]

        expected_source_name = f"{config['table_prefix']}{entity_name}{config['table_suffix']}"
        if expected_source_name not in discovered_tables:
            logger.warning(f"Source table '{expected_source_name}' not found; skipping {entity_name}")
            metrics.record_table(
                entity_name, status="skipped",
                error_msg=f"Source table not found: {expected_source_name}",
                duration_sec=time.time() - table_start,
            )
            continue

        try:
            logger.info(f"Processing: {entity_name} ({source_table} → {target_table})")

            watermark = "0"
            if config["processing_mode"] == "incremental":
                watermark = get_latest_watermark(spark, target_table)
                logger.info(f"  Watermark for {entity_name}: {watermark}")

            result = apply_cdc_merge(
                spark=spark,
                source_table=source_table,
                target_table=target_table,
                primary_keys=meta["primary_keys"],
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
for entity_name, meta in TABLE_REGISTRY.items():
    source_table = meta["source_table"]
    target_table = meta["target_table"]
    try:
        src_count = spark.table(source_table).count()
    except Exception:
        src_count = -1
    try:
        tgt_count = spark.table(target_table).count()
    except Exception:
        tgt_count = -1
    try:
        src_distinct_pks = spark.table(source_table).select("id").distinct().count()
    except Exception:
        src_distinct_pks = -1
    comparison_rows.append((entity_name, source_table, target_table, src_count, src_distinct_pks, tgt_count))

comparison_schema = "entity STRING, source_table STRING, target_table STRING, source_total_rows LONG, source_distinct_pks LONG, target_rows LONG"
comparison_df = spark.createDataFrame(comparison_rows, comparison_schema)

print("\nSource vs Target Row Count Comparison:")
display(comparison_df)

if pipeline_success:
    print("\n✓ Pipeline completed successfully!")
else:
    print("\n⚠ Pipeline completed with errors - check metrics above.")
    # Signal failure to the job scheduler
    dbutils.notebook.exit("FAILED")

dbutils.notebook.exit("SUCCESS")
