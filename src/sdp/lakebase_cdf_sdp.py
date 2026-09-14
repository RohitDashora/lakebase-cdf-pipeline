# =============================================================================
# Lakebase CDF -> Delta current-state via Lakeflow Declarative Pipeline (SDP)
#
# SDP alternative to the hand-rolled MERGE notebook (src/lakebase_cdf_pipeline.py).
# Uses AUTO CDC (create_auto_cdc_flow), so Lakebase's update_preimage /
# update_postimage change types are handled NATIVELY: AUTO CDC keeps the latest
# row per key by `sequence_by` (_sort_by) and only needs deletes marked. There is
# deliberately NO upsert change-type enumeration -- that class of bug (dropping
# update_postimage rows) cannot occur here.
#
# Generic and parameterized via pipeline `configuration`, mirroring the notebook:
# point it at any Lakebase CDF schema; tables are discovered by pattern and each
# table's primary key is resolved from the UC PRIMARY KEY constraint, else the
# configurable default key column, else the table is skipped.
#
# This is a plain SDP source file (NOT a Databricks notebook) referenced by
# resources/lakebase_cdf_sdp_pipeline.yml.
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _conf(key, default=None):
    try:
        return spark.conf.get(key)
    except Exception:
        if default is not None:
            return default
        raise


SOURCE_CATALOG = _conf("source_catalog")
SOURCE_SCHEMA = _conf("source_schema")
TARGET_CATALOG = _conf("target_catalog")
TARGET_SCHEMA = _conf("target_schema")
TABLE_PREFIX = _conf("table_prefix")
TABLE_SUFFIX = _conf("table_suffix")
PRIMARY_KEY_COL = _conf("primary_key_col", "id")

# CDC metadata columns emitted by Lakebase Lakehouse Sync (excluded from target)
PG_CHANGE_TYPE = "_pg_change_type"
SORT_BY = "_sort_by"
CDC_META = [PG_CHANGE_TYPE, "_pg_lsn", "_pg_xid", "_timestamp", SORT_BY]


def discover_tables():
    q = f"""
    SELECT table_name
    FROM {SOURCE_CATALOG}.information_schema.tables
    WHERE table_schema = '{SOURCE_SCHEMA}'
      AND table_name LIKE '{TABLE_PREFIX}%{TABLE_SUFFIX}'
    ORDER BY table_name
    """
    return [r.table_name for r in spark.sql(q).collect()]


def resolve_keys(table_name):
    """UC PRIMARY KEY constraint, else the default key column if present, else []."""
    q = f"""
    SELECT kcu.column_name
    FROM {SOURCE_CATALOG}.information_schema.table_constraints tc
    JOIN {SOURCE_CATALOG}.information_schema.key_column_usage kcu
      ON  tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema  = kcu.constraint_schema
      AND tc.constraint_name    = kcu.constraint_name
    WHERE tc.table_schema = '{SOURCE_SCHEMA}'
      AND tc.table_name   = '{table_name}'
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY kcu.ordinal_position
    """
    try:
        pk = [r.column_name for r in spark.sql(q).collect()]
    except Exception:
        pk = []
    if pk:
        return pk
    cols = [f.name.lower() for f in spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table_name}").schema.fields]
    return [PRIMARY_KEY_COL] if PRIMARY_KEY_COL.lower() in cols else []


def entity_name(table_name):
    n = table_name
    if n.startswith(TABLE_PREFIX):
        n = n[len(TABLE_PREFIX):]
    if n.endswith(TABLE_SUFFIX):
        n = n[:-len(TABLE_SUFFIX)]
    return n


def build(table_name):
    keys = resolve_keys(table_name)
    if not keys:
        # No resolvable primary key -> cannot key AUTO CDC; skip. Declare a UC
        # PRIMARY KEY constraint on the source table to include it.
        return
    entity = entity_name(table_name)
    src_fqn = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table_name}"
    view_name = f"_{entity}_changes"

    @dp.temporary_view(name=view_name)
    def _changes(_src=src_fqn):
        return spark.readStream.table(_src)

    target = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{entity}"
    dp.create_streaming_table(name=target)
    dp.create_auto_cdc_flow(
        target=target,
        source=view_name,
        keys=keys,
        sequence_by=F.col(SORT_BY),                      # postimage > preimage > insert
        apply_as_deletes=F.col(PG_CHANGE_TYPE) == "delete",
        except_column_list=CDC_META,
        stored_as_scd_type="1",                          # current state only
    )


for _t in discover_tables():
    build(_t)
