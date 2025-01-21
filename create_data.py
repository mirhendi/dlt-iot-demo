# Databricks notebook source
spark.sql("DROP SCHEMA IF EXISTS sadra.hq CASCADE")
spark.sql("CREATE SCHEMA IF NOT EXISTS sadra.hq")

# COMMAND ----------

from pyspark.sql.functions import col, lit, rand, round, unix_timestamp, expr, from_unixtime, explode, sequence, udf
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, BooleanType, TimestampType, StringType
from datetime import datetime, timedelta
import time
import random

# Parameters
num_meters = 6000  # Total meters
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
        df_duplicates = iot_hydro_data_set_1.limit(100) # Add duplicates 
    else:
        last_reading_timestamp = start_time
        df_duplicates = None
    
    current_time = last_reading_timestamp + timedelta(days=1)

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

    else:
        df_final = df_data
    
    # Append the current batch to the final table
    df_final.write.mode("append").saveAsTable(f"sadra.hq.{table_name}")


# COMMAND ----------

while True:
  simulate_iot_data("iot_source_1")
  #simulate_iot_data("iot_source_2")
  #simulate_iot_data("iot_source_3")
  time.sleep(10)

# COMMAND ----------

display(spark.read.table("sadra.hq.iot_source_1"))

# COMMAND ----------


def simulate_voltage_update():
  if not spark.catalog.tableExists("sadra.hq.meter_voltage"): # First time
    # Generate timestamps every three months
    data = []
    current_start = start_time
    while current_start < end_time:
        current_end = current_start + timedelta(days=5)  # Approximation of three months
        data.append((current_start, current_end))
        current_start = current_end

    # Define schema
    schema = StructType([
        StructField("starting_timestamp", TimestampType(), True),
        StructField("ending_timestamp", TimestampType(), True)
    ])

    # Create DataFrame
    df_timestamps = spark.createDataFrame(data, schema)
    df_voltage = df_meters.crossJoin(df_timestamps)
    df_voltage = df_voltage.withColumn("voltage_multiplier", (rand() * 3 + 1).cast(IntegerType()))
    df_voltage.write.saveAsTable("sadra.hq.meter_voltage")    
  else: #Updates
    df_voltage = spark.read.table("sadra.hq.meter_voltage")  
    fractions = [1 / df_voltage.count(), 1 - (10 / df_voltage.count())]  # Proportion for splitting
    df_voltage_update, df_voltage_no_update = df_voltage.randomSplit(fractions, seed=42)
    df_voltage_update = df_voltage_update.withColumn("voltage_multiplier", (rand() * 3 + 1).cast(IntegerType()))
    display(df_voltage_update)
    df_voltage = df_voltage_update.union(df_voltage_no_update)
    df_voltage.write.mode('overwrite').saveAsTable("sadra.hq.meter_voltage")

simulate_voltage_update()


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE sadra.hq.meter_voltage

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
    df_topology = df_meters.crossJoin(df_timestamps)
    
    # Create a UDF to select a random value from the list
    df_topology = df_topology.withColumn("region", random_choice_udf())
    df_topology.write.saveAsTable("sadra.hq.meter_topology")
    
  else: #Updates
    df_topology = spark.read.table("sadra.hq.meter_topology")  
    fractions = [1 / df_topology.count(), 1 - (10 / df_topology.count())]  # Proportion for splitting
    df_topology_update, df_topology_no_update = df_topology.randomSplit(fractions, seed=42)
    df_topology_update = df_topology_update.withColumn("region", random_choice_udf())
    display(df_topology_update)
    df_topology = df_topology_update.union(df_topology_no_update)
    df_topology.write.mode('overwrite').saveAsTable("sadra.hq.meter_topology")

simulate_toplogy_update()

