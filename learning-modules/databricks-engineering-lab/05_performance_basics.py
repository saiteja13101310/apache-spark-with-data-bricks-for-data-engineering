# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 05 — Read plans and optimize Delta responsibly
# MAGIC Uses a tiny synthetic table; optimization gains are not measurable here.
# MAGIC Drops/recreates the example table in the active development schema.
# COMMAND ----------
from pyspark.sql import functions as F
spark.sql("DROP TABLE IF EXISTS lab_performance_orders")
rows = spark.range(0, 1000).select(
    F.col("id").cast("int").alias("order_id"),
    (F.col("id") % 10).cast("int").alias("customer_id"),
    (F.col("id") * 2).cast("double").alias("amount"))
rows.write.format("delta").mode("overwrite").saveAsTable("lab_performance_orders")
# COMMAND ----------
query = spark.table("lab_performance_orders").filter("customer_id = 3")
query.explain("formatted")
display(query.limit(10))
# COMMAND ----------
# This compacts small files and may improve scan behavior for real large datasets.
# ZORDER availability and recommendations depend on runtime/table capabilities.
spark.sql("OPTIMIZE lab_performance_orders")
spark.sql("ANALYZE TABLE lab_performance_orders COMPUTE STATISTICS")
display(spark.sql("DESCRIBE DETAIL lab_performance_orders"))
assert spark.table("lab_performance_orders").count() == 1000
# MAGIC %md
# MAGIC **Production choices:** use query profiles and table metrics first.
# MAGIC Avoid blind repartitioning, excessive small files and unnecessary OPTIMIZE runs.
# MAGIC On some managed tables, predictive optimization/automatic clustering is preferable.
