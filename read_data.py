# Databricks notebook source
data = spark.read.table("sadra.hq.iot_scd_joined")
display(data)
print(data.select("reading_date").distinct().count())



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
# MAGIC   message LIKE '%iot_scd_joined%' 
# MAGIC ORDER BY
# MAGIC   timestamp desc
# MAGIC LIMIT 100
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   message,
# MAGIC   details
# MAGIC FROM
# MAGIC   event_log(TABLE(sadra.hq.iot_scd))
# MAGIC WHERE
# MAGIC   message LIKE '%voltage_scd%' AND message NOT LIKE '%NO_OP%'
# MAGIC ORDER BY
# MAGIC   timestamp desc
# MAGIC LIMIT 100
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   message,
# MAGIC   details
# MAGIC FROM
# MAGIC   event_log(TABLE(sadra.hq.iot_scd))
# MAGIC WHERE
# MAGIC   event_type = 'planning_information' AND message LIKE '%iot_hour%' AND message NOT LIKE '%NO_OP%'
# MAGIC ORDER BY
# MAGIC   timestamp desc
# MAGIC LIMIT 50
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, max as spark_max

df = spark.read.table("sadra.hq.transformer_scd")
max_update_timestamp = df.agg(spark_max("update_timestamp")).collect()[0][0]
df_filtered = df.filter(col("update_timestamp") == max_update_timestamp)
display(df_filtered)
