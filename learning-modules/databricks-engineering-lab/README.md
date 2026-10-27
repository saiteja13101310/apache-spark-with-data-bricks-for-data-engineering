# Databricks Engineering Lab

Five hands-on notebooks designed for readers learning real-world ETL and Delta Lake practices.

> **Run in a development catalog/schema only.** Every notebook creates and drops its own `lab_*` Delta tables. All records are synthetic. These are illustrative labs, not production-ready ingestion frameworks.

| Lab | File | Key lesson |
|---|---|---|
| 01 | [Delta CRUD](01_delta_table_fundamentals.py) | Delta write, update, delete, history |
| 02 | [Incremental watermark](02_incremental_watermark.py) | Saved checkpoint, tie-breaking, replay |
| 03 | [Data quality](03_data_quality_checks.py) | Quarantine null, duplicate and invalid rows |
| 04 | [Ordered CDC](04_cdc_ordering.py) | Event ordering, tombstones, late events |
| 05 | [Performance basics](05_performance_basics.py) | Query plans and OPTIMIZE |

## Running

1. Select an isolated development catalog and schema in Databricks.
2. Import or sync any `.py` file as a Databricks source notebook.
3. Attach compatible compute and run from top to bottom.
4. Review each output, then edit the synthetic input records and rerun.

## Concepts in practice

- **Full refresh** rewrites a snapshot; **incremental processing** reads a subset.
- **MERGE** applies source changes into Delta targets.
- **Data quality gates** prevent known bad rows from entering curated tables.
- **CDC** needs robust ordering and delete semantics, not simply append.
- **Performance tuning** should follow actual measured query plans and metrics.

Start with the [nine loading patterns guide](../databricks-data-loading-patterns/README.md) to understand the fundamental approaches before working through these labs.
