# Databricks Data Loading Patterns — Hands-On Guide

Learn **9 commonly used loading patterns** with runnable **PySpark + Delta Lake + Databricks SQL** examples. Every lesson creates its own synthetic customer table, shows the output and performs a small assertion.

## Run it

1. Open your Databricks workspace and select a development catalog and schema where you can **CREATE**, **DROP**, and **MODIFY** tables.
2. Import [`data_loading_patterns.py`](data_loading_patterns.py) as a Databricks notebook (or sync the file through Databricks Git folders).
3. Attach supported Databricks compute / serverless, then **Run All**. No external datasets or secrets are needed.
4. Inspect each table output and change the `initial` and `changed` input rows to experiment.

> ⚠️ The notebook drops/recreates tables called `demo_load_*` in your current schema. **Run in an isolated development schema only.** It is an educational sample, **not a production ingestion framework**.

## What you will see

| Pattern | Approach in notebook | Existing rows | Practical purpose |
|---|---|---|---|
| 1. Full load | Read all source rows, append to an empty target | Target starts empty | Initial migration or first ingest |
| 2. Truncate and load | `TRUNCATE TABLE` then append | Deleted before insert | Legacy batch replacement |
| 3. Full refresh | Delta overwrite | Replaced | Rebuild from trusted snapshot |
| 4. Incremental | Changed batch + `MERGE` | Unchanged rows retained | Daily new/changed records |
| 5. Append | Delta append | Preserved | Event/log ingestion |
| 6. Upsert / merge | `MERGE` matched update / unmatched insert | Updated or retained | Customer or order state |
| 7. CDC | Synthetic I/U/D operation feed + `MERGE` | Includes deletes | Replica of source changes |
| 8. SCD Type 1 | `MERGE` overwrite | No old attribute values | Correct customer details |
| 9. SCD Type 2 | Expire current row, insert new version | Full version history | Audit/history dimension |

### One dataset, easy comparison

**Original**: `(1, Ana, Dallas)`, `(2, Ben, Austin)`  
**Incoming**: `(1, Ana, Houston)`, `(3, Cara, Plano)`

- **Append** → 4 rows; two records with ID 1.
- **Upsert / SCD1 / Incremental** → 3 rows; latest Ana + unchanged Ben + new Cara.
- **Full refresh / Truncate** → 2 rows; Ben disappears because absent from the source snapshot.
- **CDC** → 2 rows; Ana updated, Ben deleted, Cara added.
- **SCD2** → 4 historical rows: old Ana, new Ana, Ben, Cara; 3 current rows.

## Key distinctions (important for interviews)

- **Full load** says **all source data** is loaded, not whether the target is overwritten. **Full refresh** says target is replaced. **Truncate and load** is one implementation of replacement, but separates deletion from writing; Delta overwrite is a safer atomic replacement for supported full-overwrite use cases.
- **Incremental** is the scope of input; **merge** is one way to apply it. Incremental batches may also be append-only.
- **Append** does not guarantee uniqueness or prevent duplicates on retry.
- **CDC** is a **change-capture mechanism** (insert/update/delete events); MERGE is an application strategy. Here the CDC events are simulated, **not** read from the Databricks Change Data Feed. Real CDC systems must handle ordering, late arrivals, duplicates and deletes.
- **SCD1/SCD2** are dimensional history models, **not** mutually exclusive alternatives to full/incremental processing.
- **SCD2** requires business keys, effective dating, current-row constraints, null-safe comparisons and replay/concurrency handling. The notebook stages changes before updates but uses separate transactions, so do not use unchanged in production.

## Output check

| Table | Expected rows |
|---|---:|
| demo_load_full | 2 |
| demo_load_truncate | 2 |
| demo_load_refresh | 2 |
| demo_load_incremental | 3 |
| demo_load_append | 4 |
| demo_load_upsert | 3 |
| demo_load_cdc | 2 |
| demo_load_scd1 | 3 |
| demo_load_scd2 | 4 |

## Challenge ideas

1. Make full refresh use an explicit target schema and validate row counts before swapping production data.
2. Add ingestion timestamps, deduplication, schema evolution and retry-safe MERGE logic.
3. Implement a watermark using a persisted control table instead of assuming a pre-filtered incremental batch.
4. Implement ordered CDC using Databricks Lakeflow declarative pipelines (where available).
5. Make SCD2 idempotent and transactionally robust for your runtime before using with jobs.

**Technology:** Databricks notebooks, PySpark, Spark SQL, Delta Lake. All examples are self-contained.
