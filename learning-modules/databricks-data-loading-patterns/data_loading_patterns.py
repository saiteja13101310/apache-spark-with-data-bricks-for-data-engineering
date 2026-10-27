# Databricks notebook source
# MAGIC %md
# MAGIC # 9 Data Loading Patterns in Databricks
# MAGIC Run cells top to bottom on a Databricks cluster or serverless notebook.
# MAGIC Each example uses isolated managed Delta tables with small synthetic datasets.
# MAGIC Tables are created in the current catalog/schema; set your notebook's catalog/schema first.
# MAGIC **WARNING:** Demo tables named `demo_load_*` are dropped and recreated.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from delta.tables import DeltaTable

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
])
def df(rows):
    return spark.createDataFrame(rows, schema)

def reset_table(name, rows):
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    df(rows).write.format("delta").mode("overwrite").saveAsTable(name)

def show_table(name):
    print(f"---- {name} ----")
    spark.table(name).orderBy("id").show(truncate=False)

initial = [(1, "Ana", "Dallas"), (2, "Ben", "Austin")]
changed = [(1, "Ana", "Houston"), (3, "Cara", "Plano")]
print("Initial data:", initial)
print("Incoming data:", changed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Full load
# MAGIC Reads the complete source. This example writes to an empty target.
# MAGIC A full load describes *how much source data is read*, not inherently what happens to the target.

# COMMAND ----------
reset_table("demo_load_full", [])
df(initial).write.format("delta").mode("append").saveAsTable("demo_load_full")
show_table("demo_load_full")
assert spark.table("demo_load_full").count() == 2

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Truncate and load
# MAGIC Clear existing rows first, then insert all source rows. Table definition remains.
# MAGIC **Non-atomic two-step demo:** if the insert fails, the target is empty.
# MAGIC Prefer an atomic overwrite for a production full replacement.

# COMMAND ----------
reset_table("demo_load_truncate", initial)
spark.sql("TRUNCATE TABLE demo_load_truncate")
df(changed).write.format("delta").mode("append").saveAsTable("demo_load_truncate")
show_table("demo_load_truncate")
assert {r.id for r in spark.table("demo_load_truncate").collect()} == {1, 3}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Full refresh
# MAGIC Overwrite all target data with the complete new snapshot.
# MAGIC Here we use a Delta overwrite transaction rather than separately clearing the table.

# COMMAND ----------
reset_table("demo_load_refresh", initial)
df(changed).write.format("delta").mode("overwrite").saveAsTable("demo_load_refresh")
show_table("demo_load_refresh")
assert spark.table("demo_load_refresh").count() == 2

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Incremental load
# MAGIC Incremental means processing only changed/new rows, generally via watermarks, CDC, or a change feed.
# MAGIC This demo assumes the source has already supplied a changed-row batch.
# MAGIC MERGE applies that batch so rerunning the same batch does not duplicate rows.

# COMMAND ----------
reset_table("demo_load_incremental", initial)
df(changed).createOrReplaceTempView("incoming_incremental")
spark.sql("""
MERGE INTO demo_load_incremental AS t
USING incoming_incremental AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.city = s.city
WHEN NOT MATCHED THEN INSERT (id, name, city) VALUES (s.id, s.name, s.city)
""")
show_table("demo_load_incremental")
assert spark.table("demo_load_incremental").count() == 3

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Append
# MAGIC Append adds every row and does not modify or deduplicate existing records.
# MAGIC Duplicate IDs are possible unless you explicitly validate/deduplicate.

# COMMAND ----------
reset_table("demo_load_append", initial)
df(changed).write.format("delta").mode("append").saveAsTable("demo_load_append")
show_table("demo_load_append")
assert spark.table("demo_load_append").count() == 4

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Upsert / merge
# MAGIC Existing matching IDs are updated; new IDs are inserted.
# MAGIC Result: Ana moves to Houston; Ben stays; Cara is added.

# COMMAND ----------
reset_table("demo_load_upsert", initial)
df(changed).createOrReplaceTempView("incoming_upsert")
spark.sql("""
MERGE INTO demo_load_upsert t USING incoming_upsert s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.name=s.name, t.city=s.city
WHEN NOT MATCHED THEN INSERT (id, name, city) VALUES (s.id, s.name, s.city)
""")
show_table("demo_load_upsert")
assert spark.table("demo_load_upsert").count() == 3

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. CDC load (simulated source event feed)
# MAGIC CDC communicates INSERT, UPDATE and DELETE operations.
# MAGIC Real CDC requires a real event source (e.g., change feed or log-based connector).
# MAGIC These are deliberately pre-captured events, not Databricks Change Data Feed itself.
# MAGIC Batch assumes one latest event per ID and an already correctly ordered source feed.

# COMMAND ----------
reset_table("demo_load_cdc", initial)
events_schema = "id INT, name STRING, city STRING, op STRING"
events = spark.createDataFrame([
    (1, "Ana", "Houston", "U"), (2, None, None, "D"), (3, "Cara", "Plano", "I")
], events_schema)
events.createOrReplaceTempView("incoming_cdc")
spark.sql("""
MERGE INTO demo_load_cdc t USING incoming_cdc s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op IN ('I', 'U') THEN UPDATE SET t.name=s.name, t.city=s.city
WHEN NOT MATCHED AND s.op IN ('I', 'U') THEN INSERT (id, name, city)
VALUES (s.id, s.name, s.city)
""")
show_table("demo_load_cdc")
assert {r.id for r in spark.table("demo_load_cdc").collect()} == {1, 3}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. SCD Type 1
# MAGIC Dimension attributes are overwritten; historical city values are not retained.
# MAGIC Uses MERGE so new customers are also inserted.

# COMMAND ----------
reset_table("demo_load_scd1", initial)
df(changed).createOrReplaceTempView("incoming_scd1")
spark.sql("""
MERGE INTO demo_load_scd1 t USING incoming_scd1 s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET t.name=s.name, t.city=s.city
WHEN NOT MATCHED THEN INSERT (id, name, city) VALUES (s.id, s.name, s.city)
""")
show_table("demo_load_scd1")
assert spark.table("demo_load_scd1").filter("id=1 AND city='Houston'").count() == 1

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. SCD Type 2
# MAGIC Retain prior attribute versions with effective dates and an active flag.
# MAGIC We stage only truly new/changed rows BEFORE expiring existing versions.
# MAGIC This teaching implementation uses two writes (not atomic together); production
# MAGIC designs need concurrency protection and replay-safe processing.

# COMMAND ----------
spark.sql("DROP TABLE IF EXISTS demo_load_scd2")
spark.sql("""
CREATE TABLE demo_load_scd2 (
 id INT, name STRING, city STRING, valid_from DATE, valid_to DATE, is_current BOOLEAN
) USING DELTA
""")
spark.sql("""
INSERT INTO demo_load_scd2 VALUES
 (1, 'Ana', 'Dallas', DATE '2026-01-01', NULL, true),
 (2, 'Ben', 'Austin', DATE '2026-01-01', NULL, true)
""")
df(changed).createOrReplaceTempView("incoming_scd2")
# Materialize changes to a demo Delta table BEFORE modifying the dimension.
spark.sql("DROP TABLE IF EXISTS demo_load_scd2_changes")
spark.sql("""
CREATE TABLE demo_load_scd2_changes USING DELTA AS
SELECT s.id, s.name, s.city
FROM incoming_scd2 s
LEFT JOIN demo_load_scd2 t ON s.id=t.id AND t.is_current=true
WHERE t.id IS NULL OR NOT (t.name <=> s.name AND t.city <=> s.city)
""")
spark.sql("""
MERGE INTO demo_load_scd2 t
USING demo_load_scd2_changes s
ON t.id=s.id AND t.is_current=true
WHEN MATCHED THEN UPDATE SET
 t.valid_to = DATE '2026-10-27', t.is_current=false
""")
spark.sql("""
INSERT INTO demo_load_scd2 (id, name, city, valid_from, valid_to, is_current)
SELECT id, name, city, DATE '2026-10-27', NULL, true
FROM demo_load_scd2_changes
""")
spark.table("demo_load_scd2").orderBy("id", "valid_from", "city").show(truncate=False)
assert spark.table("demo_load_scd2").filter("id=1").count() == 2
assert spark.table("demo_load_scd2").filter("is_current=true").count() == 3
print("All nine demonstrations completed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Further exercises
# MAGIC 1. Rerun Append and Incremental cells. What happens to row counts?
# MAGIC 2. Modify the incoming batch so Ana's city remains Dallas. How does SCD2 change?
# MAGIC 3. Extend CDC events with an event timestamp and deduplicate by key using window functions.
# MAGIC 4. Replace the hardcoded date with a job parameter and implement replay safety.
# MAGIC
# MAGIC **Cleanup (optional):** only run when you no longer need demo data.
# COMMAND ----------
# Uncomment to remove ALL demo tables:
# for t in ["full","truncate","refresh","incremental","append","upsert","cdc","scd1","scd2","scd2_changes"]:
#     spark.sql(f"DROP TABLE IF EXISTS demo_load_{t}")
