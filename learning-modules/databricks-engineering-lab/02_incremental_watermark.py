# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 02 — Incremental load with a persisted watermark
# MAGIC Simulates an initial load and subsequent batch. Timestamp ties are handled with
# MAGIC `(updated_at, id)` ordering; real pipelines need a source change token/CDC when
# MAGIC timestamps are non-unique or records can arrive late.
# MAGIC Run in a disposable development schema; resets lab_watermark_* tables.
# COMMAND ----------
from pyspark.sql import functions as F
spark.sql("DROP TABLE IF EXISTS lab_watermark_target")
spark.sql("DROP TABLE IF EXISTS lab_watermark_control")
spark.sql("CREATE TABLE lab_watermark_target (id INT, item STRING, updated_at TIMESTAMP) USING DELTA")
spark.sql("""CREATE TABLE lab_watermark_control (
 pipeline STRING, last_updated_at TIMESTAMP, last_id INT
) USING DELTA""")
spark.sql("""INSERT INTO lab_watermark_control
VALUES ('inventory', TIMESTAMP '1900-01-01 00:00:00', 0)""")
# COMMAND ----------
source = spark.createDataFrame([
    (1, "pens", "2026-01-01 09:00:00"),
    (2, "books", "2026-01-01 09:00:00"),
    (3, "pencils", "2026-01-02 10:00:00")
], "id INT, item STRING, updated_at STRING").withColumn("updated_at", F.to_timestamp("updated_at"))
source.createOrReplaceTempView("lab_inventory_source")
# COMMAND ----------
def apply_batch():
    checkpoint = spark.sql("""SELECT last_updated_at, last_id
        FROM lab_watermark_control WHERE pipeline='inventory'""").first()
    filtered = spark.table("lab_inventory_source").where(
        (F.col("updated_at") > F.lit(checkpoint.last_updated_at)) |
        ((F.col("updated_at") == F.lit(checkpoint.last_updated_at)) &
         (F.col("id") > F.lit(checkpoint.last_id)))
    )
    filtered.createOrReplaceTempView("lab_inventory_changes")
    spark.sql("""MERGE INTO lab_watermark_target t USING lab_inventory_changes s ON t.id=s.id
      WHEN MATCHED THEN UPDATE SET t.item=s.item, t.updated_at=s.updated_at
      WHEN NOT MATCHED THEN INSERT (id,item,updated_at) VALUES (s.id,s.item,s.updated_at)""")
    latest = filtered.orderBy(F.col("updated_at").desc(), F.col("id").desc()).first()
    if latest:
        spark.sql(f"""UPDATE lab_watermark_control
            SET last_updated_at = TIMESTAMP '{latest.updated_at}', last_id = {latest.id}
            WHERE pipeline='inventory'""")
# COMMAND ----------
apply_batch()
apply_batch()  # replay safe for this unchanged ordered source
display(spark.table("lab_watermark_target").orderBy("id"))
assert spark.table("lab_watermark_target").count() == 3
# MAGIC %md
# MAGIC **Production note:** MERGE and watermark updates are two transactions.
# MAGIC If the checkpoint update fails, replay is safe only because the merge is idempotent.
# MAGIC Late records with older timestamps may be missed; use CDC/change feed for correctness.
