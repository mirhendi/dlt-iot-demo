# Databricks notebook source
data = spark.read.table("sadra.hq.iot_scd_joined")
display(data)
print(data.count())



# COMMAND ----------

data = spark.read.table("sadra.hq.iot_data_agg_by_hour")
display(data)
print(data.count())

data = spark.read.table("sadra.hq.iot_data_agg_by_day")
display(data)
print(data.count())

# COMMAND ----------

from pyspark.sql.functions import count

duplicate_rows = data.groupBy("meter_id", "reading_timestamp").agg(count("*").alias("count")).filter("count > 1")
display(duplicate_rows)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   message,
# MAGIC   details
# MAGIC FROM
# MAGIC   event_log(TABLE(sadra.hq.iot_scd))
# MAGIC WHERE
# MAGIC   event_type = 'planning_information'
# MAGIC ORDER BY
# MAGIC   timestamp desc
# MAGIC LIMIT 10
# MAGIC
