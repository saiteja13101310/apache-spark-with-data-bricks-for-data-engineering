# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 04 — CDC event ordering and replay-safe MERGE
# MAGIC The highest sequence number per key wins in each incoming batch.
# MAGIC A persisted per-key sequence prevents older events overwriting newer state.
# MAGIC Drops and recreates `lab_cdc_state` in your current development schema.
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
spark.sql("DROP TABLE IF EXISTS lab_cdc_state")
spark.sql("""CREATE TABLE lab_cdc_state (
 id INT, city STRING, last_seq BIGINT, is_deleted BOOLEAN
) USING DELTA""")
# COMMAND ----------
def apply_events(events):
    frame = spark.createDataFrame(events, "id INT, city STRING, seq LONG, op STRING")
    # One event per (id, seq) and no conflicting payload is an input contract.
    window = Window.partitionBy("id").orderBy(F.col("seq").desc())
    latest = frame.withColumn("rn", F.row_number().over(window)).filter("rn=1").drop("rn")
    latest.createOrReplaceTempView("lab_cdc_latest")
    spark.sql("""MERGE INTO lab_cdc_state t USING lab_cdc_latest s ON t.id=s.id
      WHEN MATCHED AND s.seq > t.last_seq THEN UPDATE SET
        t.city=s.city, t.last_seq=s.seq, t.is_deleted=(s.op='D')
      WHEN NOT MATCHED THEN INSERT (id,city,last_seq,is_deleted)
        VALUES (s.id,s.city,s.seq,s.op='D')""")
# COMMAND ----------
apply_events([(1,"Dallas",1,"I"),(1,"Austin",2,"U"),(2,"Plano",1,"I")])
apply_events([(1,"Dallas",1,"I"),(2,None,3,"D"),(3,"Houston",2,"I")])
apply_events([(1,"Austin",2,"U"),(2,None,3,"D")])  # replay
display(spark.table("lab_cdc_state").orderBy("id"))
display(spark.table("lab_cdc_state").filter("is_deleted = false").orderBy("id"))
assert spark.table("lab_cdc_state").filter("is_deleted = false").count() == 2
assert spark.table("lab_cdc_state").filter("id=2 AND is_deleted=true").count() == 1
# MAGIC %md
# MAGIC Tombstones preserve the last sequence for deleted records, preventing
# MAGIC a delayed older INSERT from accidentally resurrecting a deleted key.
# MAGIC For production, validate operation types and event uniqueness.
