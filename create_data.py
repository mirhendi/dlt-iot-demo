# Databricks notebook source
first_time = False
if first_time:
  spark.sql("DROP SCHEMA IF EXISTS sadra.hq CASCADE")
  spark.sql("CREATE SCHEMA IF NOT EXISTS sadra.hq")

  spark.sql("DROP TABLE IF EXISTS sadra.hq.meter_voltage")

  # Create voltage table with specified schema and set table properties
  spark.sql("""
  CREATE TABLE sadra.hq.meter_voltage (
      meter_id LONG,
      voltage_multiplier INT,
      reading_date DATE
  )
  PARTITIONED BY (reading_date)
  TBLPROPERTIES (
      delta.enableChangeDataFeed = true,
      delta.enableRowTracking = true,
      delta.enableDeletionVectors = true)
  """)

  spark.sql("DROP TABLE IF EXISTS sadra.hq.meter_topology")



# COMMAND ----------

from pyspark.sql.functions import col, lit, rand, round, unix_timestamp, expr, from_unixtime, explode, sequence, udf
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, BooleanType, TimestampType, StringType
from datetime import datetime, timedelta
import time
import random
import pyspark.sql.functions as f

# Parameters0
num_meters = 5000000  # Total meters
start_time = datetime(2024, 1, 1)
end_time = datetime(2025, 1, 1)



# Generate a DataFrame with all meter IDs
df_meters = spark.range(100000, 100000 + num_meters).withColumnRenamed("id", "meter_id")


# COMMAND ----------


def simulate_iot_data(table_name):
    reading_interval = 15 # minutes 
    value_list = ["A", "B", "C", "D"]
    random_choice_udf = udf(lambda: random.choice(value_list), StringType())

    if spark.catalog.tableExists(f"sadra.hq.{table_name}"): # First time:
        iot_hydro_data_set_1 = spark.read.table(f"sadra.hq.{table_name}")
        last_reading_timestamp = spark.sql(f"SELECT max(reading_timestamp) FROM sadra.hq.{table_name}").collect()[0][0]
        df_duplicates = iot_hydro_data_set_1.limit(100) # Add corrections
    else:
        last_reading_timestamp = start_time
        df_duplicates = None

    current_time = last_reading_timestamp + timedelta(hours=12)
    print(last_reading_timestamp, current_time)
    # Generate a DataFrame with the current batch of timestamps
    df_timestamps = spark.sql(f"""
        SELECT explode(sequence(
            to_timestamp('{last_reading_timestamp.strftime('%Y-%m-%d %H:%M:%S')}'),
            to_timestamp('{(current_time).strftime('%Y-%m-%d %H:%M:%S')}'),
            interval {reading_interval} minutes
        )) as reading_timestamp
    """)

    # Cross join meters and timestamps to create the base dataset
    df_base = df_meters.crossJoin(df_timestamps)
    df_base = df_base.union(df_base).union(df_base).union(df_base)

    # Add random reading values and arrival timestamps
    df_data = df_base \
        .withColumn("reading_value", round(rand() * (500.0 - 10.0) + 10.0, 2)) \
        .withColumn("reading_type", random_choice_udf()) \
        .withColumn("arrival_timestamp", lit(current_time))

    if df_duplicates is not None:
        df_duplicates = df_duplicates \
            .withColumn("reading_value", round(rand() * (500.0 - 10.0) + 10.0, 2)) \
            .withColumn("reading_type", random_choice_udf()) \
            .withColumn("arrival_timestamp", lit(current_time))

        # Union the original and duplicate DataFrames
        df_final = df_data.union(df_duplicates)
        #df_final = df_data
    else:
        df_final = df_data
    display(df_final.select("reading_timestamp").distinct())
    # Append the current batch to the final table
    df_final.write.mode("append").saveAsTable(f"sadra.hq.{table_name}")


# COMMAND ----------

simulate_iot_data("iot_source_1")
#simulate_iot_data("iot_source_2")
#simulate_iot_data("iot_source_3")

#while True:
#  simulate_iot_data("iot_source_1")
#  time.sleep(300)


# COMMAND ----------

df = spark.read.table("sadra.hq.iot_source_1")
print(df.count()/1000000, "Milion rows")
display(df.limit(10))

# COMMAND ----------


def simulate_voltage_update():
  df = spark.read.table("sadra.hq.meter_voltage")
  if df.count() == 0: # First time
    # Generate timestamps every three months

    df_timestamps = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{start_time.strftime('%Y-%m-%d')}'),
            to_date('{end_time.strftime('%Y-%m-%d')}'),
            interval 1 day
        )) as reading_date
    """)
    df_voltage = df_meters.crossJoin(df_timestamps)
    df_voltage = df_voltage.withColumn("voltage_multiplier", (rand() * 3 + 1).cast(IntegerType()))
    df_voltage.write.mode("append").saveAsTable("sadra.hq.meter_voltage")
    display(df_voltage)

  else: #Updates
    df_voltage = spark.read.table("sadra.hq.meter_voltage")

    # Select a row to update
    df_voltage_update = df_voltage.orderBy(rand()).limit(1)
    df_voltage_update = df_voltage_update.withColumn("voltage_multiplier", (rand() * 3 + 1).cast(IntegerType()))
    display(df_voltage_update)
    # Create a temporary view for the row to update
    df_voltage_update.createOrReplaceTempView("df_voltage_update")

    # Merge the updated row back into the delta table
    spark.sql("""
    MERGE INTO sadra.hq.meter_voltage AS target
    USING df_voltage_update AS source
    ON target.meter_id = source.meter_id 
    AND target.reading_date = source.reading_date 
    WHEN MATCHED THEN
      UPDATE SET target.voltage_multiplier = source.voltage_multiplier
    """)

simulate_voltage_update()

# COMMAND ----------

def simulate_toplogy_update():
  value_list = ["Montreal", "Quebec City", "Laval", "Gatineau", "Longueuil"]  # Quebec city names
  random_choice_udf = udf(lambda: random.choice(value_list), StringType())

  if not spark.catalog.tableExists("sadra.hq.meter_topology"): # First time
    # Generate timestamps every three months
    data = []
    current_start = start_time
    while current_start < end_time:
        current_end = current_start + timedelta(days=90)  # Approximation of three months
        data.append((current_start, current_end))
        current_start = current_end

    # Define schema
    schema = StructType([
        StructField("starting_timestamp", TimestampType(), True),
        StructField("ending_timestamp", TimestampType(), True)
    ])

    # Create DataFrame
    df_timestamps = spark.createDataFrame(data, schema)
    df = df_meters.crossJoin(df_timestamps)

    # Create a UDF to select a random value from the list
    df = df.withColumn("region", random_choice_udf())
    df = df.withColumn("update_timestamp", f.current_timestamp())
    display(df)
    df.write.saveAsTable("sadra.hq.meter_topology")

  else: #Updates
    df = spark.read.table("sadra.hq.meter_topology")  
    df_update = df.orderBy(rand()).limit(2)
    df_no_update = df.subtract(df_update)

    # Split each updated row into two rows
    df_update_1 = df_update.withColumn("ending_timestamp", col("starting_timestamp") + expr("INTERVAL 2 DAYS"))
    df_update_2 = df_update.withColumn("starting_timestamp", col("starting_timestamp") + expr("INTERVAL 2 DAYS"))
    df_update = df_update_1.union(df_update_2)
    df_update = df_update.withColumn("region", random_choice_udf())
    df_update = df_update.withColumn("update_timestamp", f.current_timestamp())
    display(df_update)
    df = df_update.union(df_no_update)
    df.write.mode('overwrite').saveAsTable("sadra.hq.meter_topology")

simulate_toplogy_update()

