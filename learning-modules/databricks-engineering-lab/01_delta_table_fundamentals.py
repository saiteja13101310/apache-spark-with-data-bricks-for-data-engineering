# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 01 — Delta Lake: create, append, update, delete
# MAGIC Run in an isolated development schema. This notebook overwrites `lab_delta_orders`.
# COMMAND ----------
from pyspark.sql import functions as F
spark.sql("DROP TABLE IF EXISTS lab_delta_orders")
orders = spark.createDataFrame([
    (1, "placed", 120.0), (2, "placed", 75.0)
], "order_id INT, status STRING, amount DOUBLE")
orders.write.format("delta").mode("overwrite").saveAsTable("lab_delta_orders")
display(spark.table("lab_delta_orders").orderBy("order_id"))
# COMMAND ----------
spark.sql("INSERT INTO lab_delta_orders VALUES (3, 'placed', 42.0)")
spark.sql("UPDATE lab_delta_orders SET status='shipped' WHERE order_id=1")
spark.sql("DELETE FROM lab_delta_orders WHERE order_id=2")
result = spark.table("lab_delta_orders")
display(result.orderBy("order_id"))
assert result.count() == 2
assert result.filter("order_id=1 AND status='shipped'").count() == 1
# COMMAND ----------
# MAGIC %md
# MAGIC ## Investigate history
# MAGIC Delta transactions provide history and time travel within retention limits.
# COMMAND ----------
display(spark.sql("DESCRIBE HISTORY lab_delta_orders"))
