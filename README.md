# Lakebase PostgreSQL CDF → Delta Lakehouse Pipeline

A self-contained **Databricks Asset Bundle (DAB)** that deploys a fully parameterized pipeline for syncing PostgreSQL Change Data Feed (CDF) tables into Delta Lakehouse.

> **Clone → Set params → Deploy → Run.** That's it. No manual workspace setup. The bundle creates the job, syncs the notebook, wires all parameters, and configures compute in a single `databricks bundle deploy`.

---

## What It Does

```
PostgreSQL CDF History Tables          Delta Lakehouse (Current State)
┌──────────────────────────┐        ┌─────────────────────────┐
│ lb_*_history tables        │  CDC   │ Clean current-state tables  │
│ (insert/update/delete      │ MERGE  │ (deduplicated, no CDC meta, │
│  events with _pg_lsn)      │ ────▶ │  watermark-tracked)         │
└──────────────────────────┘        └─────────────────────────┘
```

1. **Discovers** all `{prefix}*{suffix}` CDF tables dynamically from `information_schema` (no hardcoded table list)
2. **Deduplicates** CDC events per primary key (keeps latest by `_sort_by`)
3. **Applies** INSERT / UPDATE / DELETE via Delta MERGE
4. **Tracks** watermarks (`_pg_lsn`) for efficient incremental processing
5. **Reports** per-table metrics (rows inserted, updated, deleted, duration)
6. **Isolates errors** per table — one table failure doesn't stop the rest

---

## Project Structure

```
lakebase-cdf-pipeline/
├── databricks.yml                  # Bundle config: variables, 5 targets
├── resources/
│   ├── lakebase_cdf_job.yml        # Pipeline job: task, compute, schedule, retry
│   └── validate_job.yml            # Native pre-flight validation job
├── src/
│   ├── lakebase_cdf_pipeline.py    # Pipeline notebook (8 cells, 14 params)
│   └── validate_bundle.py          # Validation notebook (Spark SQL checks)
├── .gitignore                      # Ignore .bundle/, .databricks/, etc.
├── LICENSE                         # Internal-use license
└── README.md                       # This file
```

---

## Prerequisites

- **Databricks CLI** v0.200+ installed (`pip install databricks-cli`)
- **Workspace authentication** configured (`databricks auth login`)
- **Unity Catalog** access to source and target catalogs
- **CREATE SCHEMA** privilege on the target catalog

---

## Quick Start (5 commands)

```bash
# 1. Clone
git clone <repo-url> lakebase-cdf-pipeline && cd lakebase-cdf-pipeline

# 2. Authenticate (one-time)
databricks auth login --host https://<workspace-url>

# 3. Validate
databricks bundle validate --target dev

# 4. Deploy
databricks bundle deploy --target dev

# 5. Run
databricks bundle run lakebase_cdf_pipeline_job --target dev
```

The deploy command creates:
- Both notebooks synced to the workspace
- A `[dev] Lakebase CDF Pipeline` job with all 14 parameters
- A `[dev] Lakebase CDF Validation` job for native pre-flight checks (see [Native Validation](#native-validation))
- Serverless compute, schedule, retry policy, and tags

---

## Deployment Modes

Choose a target based on how you want the pipeline to run:

| Target               | Run Mode        | Behavior                                                            |
| -------------------- | --------------- | ------------------------------------------------------------------- |
| `dev`                | Cron schedule   | Full reprocess, daily at 8 AM ET, **paused** (manual trigger)       |
| `staging`            | Cron schedule   | Incremental, every 4 hours, **paused** — separate workspace (see below) |
| `prod-scheduled`     | Cron schedule   | Incremental, every 2 hours via quartz cron, **paused**              |
| `prod-triggered`     | Periodic trigger| Incremental, every 1 hour via periodic trigger, **unpaused**        |
| `prod-continuous`    | Continuous      | Incremental, restarts immediately on completion, **unpaused**       |

> **`staging` targets a different workspace.** It sets `workspace.host` to
> `https://fe-vm-agent-spark.cloud.databricks.com` and reads CDF history from
> `rd_classic_catalog.geniefy_sync`. Deploying it needs a CLI profile
> authenticated to that host:
> ```bash
> databricks auth login --host https://fe-vm-agent-spark.cloud.databricks.com --profile fe-vm-v2
> databricks bundle validate --target staging -p fe-vm-v2
> databricks bundle deploy   --target staging -p fe-vm-v2
> ```
> The other targets use whichever workspace your default/`--profile` auth points at.

### Deploy Examples

```bash
# Scheduled (runs on cron, paused by default)
databricks bundle deploy --target prod-scheduled

# Triggered (runs every hour, starts automatically)
databricks bundle deploy --target prod-triggered

# Continuous (always running, restarts on completion)
databricks bundle deploy --target prod-continuous
```

### Override Variables at Deploy or Run Time

```bash
# Point to a different source
databricks bundle deploy --target dev \
  --var source_catalog=my_catalog \
  --var source_schema=my_schema \
  --var table_prefix=lb_

# One-shot full reprocess in prod
databricks bundle run lakebase_cdf_pipeline_job --target prod-scheduled \
  --var processing_mode=full
```

---

## Configuration Variables

| Variable                 | Default                       | Description                                         |
| ------------------------ | ----------------------------- | --------------------------------------------------- |
| `source_catalog`         | `rd_classic_catalog`          | UC catalog with CDF history tables                  |
| `source_schema`          | `lakebase_benchmarks`         | Schema containing `lb_*_history` tables             |
| `target_catalog`         | `rd_classic_catalog`          | UC catalog for current-state output                 |
| `target_schema`          | `lakebase_benchmarks_current` | Output schema (created if missing)                  |
| `table_prefix`           | `lb_s1tnt1_`                  | Source table naming prefix                          |
| `table_suffix`           | `_history`                    | Source table naming suffix                          |
| `primary_key_col`        | `id`                          | Default key column when a table has no UC PRIMARY KEY |
| `processing_mode`        | `incremental`                 | `incremental` (watermark) or `full` (reprocess all) |
| `enable_delete_handling` | `true`                        | Apply DELETE CDC operations to target               |
| `schedule_cron`          | `0 0 */2 * * ?`               | Quartz cron (schedule-based targets only)           |
| `email_notifications`    | `{}` _(no alerts)_            | Complex var; override to enable failure alerts (see below) |

### Enabling Failure Alerts

`email_notifications` is a complex variable holding the job's notification block. It defaults to `{}` — no alerts, and no invalid empty recipient. Enable alerts by overriding it under the target you deploy:

```yaml
targets:
  prod-scheduled:
    variables:
      email_notifications:
        on_failure:
          - alerts@company.com
```

> **Note:** complex variables must be set in the bundle config (as above) — the `--var` CLI flag does not accept inline JSON for them.

---

## Table Discovery & Primary Keys

There is **no hardcoded table list** — point the bundle at any Lakebase CDF
schema and it builds the pipeline. At run time the pipeline:

1. **Discovers** every table matching `{table_prefix}*{table_suffix}` in the
   source schema (via `information_schema`).
2. Derives each target table's name by stripping the prefix/suffix
   (e.g. `lb_sessions_history` → `sessions`).
3. **Resolves each table's primary key generically** — no per-table logic:

   | Order | Source of the key | Notes |
   | ----- | ----------------- | ----- |
   | 1 | UC `PRIMARY KEY` constraint | Authoritative; supports composite keys |
   | 2 | The `primary_key_col` default (`id`) | Used only if that column exists on the table |
   | 3 | _(none)_ | Table is **skipped** and reported — can't merge without a key |

To **include a table**, make sure it matches the prefix/suffix and has a
resolvable key: either declare a UC primary key —

```sql
ALTER TABLE <catalog>.<schema>.<prefix><entity><suffix>
  ADD CONSTRAINT <entity>_pk PRIMARY KEY (<col> [, <col> ...]);
```

— or ensure it has the default `primary_key_col` column. Nothing else to edit;
redeploy is only needed if you change bundle variables.

---

## How the CDC Merge Works

```
Source _history table
    │
    ├─ 1. Filter by watermark (_pg_lsn > last processed)
    ├─ 2. Deduplicate per PK (keep highest _sort_by)
    ├─ 3. Split: upserts (insert/update) vs deletes
    ├─ 4. Strip 5 CDC metadata columns, add 2 audit columns
    │
    └─ 5. Delta MERGE into target
         ├─ MATCHED + delete  → DELETE row
         ├─ MATCHED + upsert  → UPDATE SET *
         └─ NOT MATCHED       → INSERT *
```

**Supported change types:**

| Format                 | Insert    | Update    | Delete    |
| ---------------------- | --------- | --------- | --------- |
| Lakebase CDF (words)   | `insert`  | `update`  | `delete`  |
| PG Logical Replication | `I` / `c` | `U` / `u` | `D` / `d` |

**CDC metadata columns stripped:** `_pg_change_type`, `_pg_lsn`, `_pg_xid`, `_timestamp`, `_sort_by`

**Audit columns added:** `_last_pg_lsn`, `_last_cdc_timestamp`

---

## Customization Guide

### Use Different Source Tables

Change `table_prefix` to match your naming convention:

```bash
databricks bundle deploy --target dev --var table_prefix=myprefix_
```

### Change How Primary Keys Are Resolved

Keys are resolved per table (UC constraint → `primary_key_col` default → skip),
so there's nothing table-specific to edit. To key a table differently, declare a
UC `PRIMARY KEY` constraint on it (wins over the default). To change the default
key column used when no constraint exists:

```bash
databricks bundle deploy --target dev --var primary_key_col=my_id_col
```

### Point to a Different Workspace

```bash
databricks auth login --host https://different-workspace.cloud.databricks.com
databricks bundle deploy --target prod-scheduled
```

### Native Validation

Validation is **Databricks-native** — it runs on the same serverless compute as
the pipeline, no external CI runner required. The bundle ships a second job,
`bundle_validate_job` (`resources/validate_job.yml` → `src/validate_bundle.py`),
that checks the live preconditions the pipeline depends on and **fails the run**
on any problem, so it works as a native CI gate.

It validates:

1. Source catalog / schema are reachable
2. At least one `{prefix}*{suffix}` CDF history table is discoverable
3. Each discovered table honors the **CDC schema contract** — `_pg_change_type`
   is a string, `_pg_lsn` and `_sort_by` are integral (required for the numeric
   watermark comparison), `_timestamp` is a timestamp, and the primary key exists
4. The target catalog is reachable (and whether the target schema already exists)

```bash
# Validate the deployed target's preconditions (deploy once, then run)
databricks bundle deploy --target dev
databricks bundle run bundle_validate_job --target dev
```

Run it before deploying/running the pipeline for real, or add a `schedule:` block
to `resources/validate_job.yml` to run it periodically. A failed run means a
precondition is broken (missing table, schema drift, unreachable target).

> **Two layers of validation.** `databricks bundle validate` is a fast,
> client-side check of the bundle *config* (run it locally before deploy); the
> `bundle_validate_job` above checks the live *workspace and data* preconditions
> from inside Databricks.

Failure-alert recipients are configured in the target's `email_notifications`
block in `databricks.yml` (see [Enabling Failure Alerts](#enabling-failure-alerts)).

---

## License

Internal use. Modify freely for your organization.
