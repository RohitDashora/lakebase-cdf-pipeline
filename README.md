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

1. **Discovers** all `lb_*_history` CDF tables dynamically from `information_schema`
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
│   └── lakebase_cdf_job.yml        # Job: task, compute, schedule, retry
├── src/
│   └── lakebase_cdf_pipeline.py    # Pipeline notebook (8 cells, 15 params)
├── .github/
│   └── workflows/
│       └── validate.yml            # CI: lint + bundle validate on push/PR
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
- A notebook synced to the workspace
- A `[dev] Lakebase CDF Pipeline` job with all 15 parameters
- Serverless compute, schedule, retry policy, and tags

---

## Deployment Modes

Choose a target based on how you want the pipeline to run:

| Target               | Run Mode        | Behavior                                                            |
| -------------------- | --------------- | ------------------------------------------------------------------- |
| `dev`                | Cron schedule   | Full reprocess, daily at 8 AM ET, **paused** (manual trigger)       |
| `staging`            | Cron schedule   | Incremental, every 4 hours, **paused**                              |
| `prod-scheduled`     | Cron schedule   | Incremental, every 2 hours via quartz cron, **paused**              |
| `prod-triggered`     | Periodic trigger| Incremental, every 1 hour via periodic trigger, **unpaused**        |
| `prod-continuous`    | Continuous      | Incremental, restarts immediately on completion, **unpaused**       |

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
| `processing_mode`        | `incremental`                 | `incremental` (watermark) or `full` (reprocess all) |
| `enable_delete_handling` | `true`                        | Apply DELETE CDC operations to target               |
| `schedule_cron`          | `0 0 */2 * * ?`               | Quartz cron (schedule-based targets only)           |
| `notification_email`     | _(empty)_                     | Email for failure alerts                            |

---

## Source Tables (Default)

| Entity              | Source History Table                      | Target Table          |
| ------------------- | ----------------------------------------- | --------------------- |
| profile             | `lb_s1tnt1_profile_history`               | `profile`             |
| contactpointaddress | `lb_s1tnt1_contactpointaddress_history`   | `contactpointaddress` |
| contactpointemail   | `lb_s1tnt1_contactpointemail_history`     | `contactpointemail`   |
| contactpointphone   | `lb_s1tnt1_contactpointphone_history`     | `contactpointphone`   |
| contactpointsocial  | `lb_s1tnt1_contactpointsocial_history`    | `contactpointsocial`  |
| education           | `lb_s1tnt1_education_history`             | `education`           |
| interest            | `lb_s1tnt1_interest_history`              | `interest`            |
| preference          | `lb_s1tnt1_preference_history`            | `preference`          |
| subscription        | `lb_s1tnt1_subscription_history`          | `subscription`        |
| alternatekey        | `lb_s1tnt1_alternatekey_history`          | `alternatekey`        |

---

## Adding New Tables

1. Create the CDF history table following `{table_prefix}{entity}{table_suffix}` naming
2. Add to `TABLE_REGISTRY` in `src/lakebase_cdf_pipeline.py`:
   ```python
   "new_entity": {"primary_keys": ["id"], "unique_business_key": "new_entity_id"},
   ```
3. Redeploy: `databricks bundle deploy --target <target>`

The pipeline discovers tables dynamically — any table matching the prefix/suffix pattern is picked up.

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

### Use a Different Primary Key

Edit `TABLE_REGISTRY` in the notebook — each entity can have its own `primary_keys` list.

### Point to a Different Workspace

```bash
databricks auth login --host https://different-workspace.cloud.databricks.com
databricks bundle deploy --target prod-scheduled
```

### CI/CD Integration

This repo ships a GitHub Actions workflow (`.github/workflows/validate.yml`) that:

- **Lints** the notebook (Python syntax) and bundle YAML on every push and PR — no credentials required.
- **Validates** all five bundle targets with `databricks bundle validate` — runs only when the repo has `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets set (skipped cleanly otherwise).

To enable the validate job, add two repository secrets (Settings → Secrets and variables → Actions):

| Secret             | Value                                            |
| ------------------ | ------------------------------------------------ |
| `DATABRICKS_HOST`  | `https://<your-workspace>.cloud.databricks.com`  |
| `DATABRICKS_TOKEN` | A workspace personal access token                |

To deploy from CI, extend the workflow with a deploy step:

```yaml
- name: Deploy Pipeline
  run: |
    databricks bundle deploy --target prod-scheduled \
      --var notification_email=${{ secrets.ALERT_EMAIL }}
```

---

## License

Internal use. Modify freely for your organization.
