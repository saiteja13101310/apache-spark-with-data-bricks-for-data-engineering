# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 03 — Data quality gates before writing to Delta
# MAGIC Demonstrates null IDs, duplicates, and invalid amounts. Invalid rows are quarantined.
# MAGIC This lab drops/recreates its own tables in your current development schema.
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
spark.sql("DROP TABLE IF EXISTS lab_dq_valid")
spark.sql("DROP TABLE IF EXISTS lab_dq_rejected")
incoming = spark.createDataFrame([
    (1, "apple", 12.0), (2, "orange", 4.0), (2, "orange duplicate", 6.0),
    (None, "bad ID", 3.0), (4, "negative", -1.0)
], "id INT, item STRING, price DOUBLE")
# COMMAND ----------
w = Window.partitionBy("id")
checked = incoming.withColumn("duplicate_count", F.count("*").over(w)).withColumn(
    "rejection_reason",
    F.when(F.col("id").isNull(), F.lit("NULL_ID"))
     .when(F.col("duplicate_count") > 1, F.lit("DUPLICATE_ID"))
     .when(F.col("price").isNull() | (F.col("price") < 0), F.lit("INVALID_PRICE"))
)
valid = checked.filter("rejection_reason IS NULL").select("id","item","price")
rejected = checked.filter("rejection_reason IS NOT NULL").drop("duplicate_count")
valid.write.format("delta").mode("overwrite").saveAsTable("lab_dq_valid")
rejected.write.format("delta").mode("overwrite").saveAsTable("lab_dq_rejected")
display(spark.table("lab_dq_valid"))
display(spark.table("lab_dq_rejected"))
assert spark.table("lab_dq_valid").count() == 1
assert spark.table("lab_dq_rejected").count() == 4
# MAGIC %md
# MAGIC **Why quarantine?** Bad inputs should be observable and repairable.
# MAGIC Decide your policy before production: reject the entire batch, quarantine,
# MAGIC or use a trusted deduplication tie-breaker (event time/sequence number).
