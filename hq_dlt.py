# Databricks notebook source
import dlt
import pyspark.sql.functions as f

@dlt.view(name="source_1")
def transformed_streams_1():
    df = spark.readStream.table("sadra.hq.iot_source_1")

    df = df.withColumn("reading_type_A", f.when(f.col("reading_type") == "A", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_B", f.when(f.col("reading_type") == "B", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_C", f.when(f.col("reading_type") == "C", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_D", f.when(f.col("reading_type") == "D", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_date", f.col("reading_timestamp").cast("date"))
    df = df.withColumn("reading_hour", f.hour(f.col("reading_timestamp")))
    return df

@dlt.view(name="source_2")
def transformed_streams_2():
    df = spark.readStream.table("sadra.hq.iot_source_2")

    df = df.withColumn("reading_type_A", f.when(f.col("reading_type") == "A", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_B", f.when(f.col("reading_type") == "B", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_C", f.when(f.col("reading_type") == "C", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_D", f.when(f.col("reading_type") == "D", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_date", f.col("reading_timestamp").cast("date"))
    df = df.withColumn("reading_hour", f.hour(f.col("reading_timestamp")))
    return df

@dlt.view(name="source_3")
def transformed_streams_3():
    df = spark.readStream.table("sadra.hq.iot_source_3")

    df = df.withColumn("reading_type_A", f.when(f.col("reading_type") == "A", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_B", f.when(f.col("reading_type") == "B", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_C", f.when(f.col("reading_type") == "C", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_D", f.when(f.col("reading_type") == "D", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_date", f.col("reading_timestamp").cast("date"))
    df = df.withColumn("reading_hour", f.hour(f.col("reading_timestamp")))
    return df

dlt.create_streaming_table(name="iot_scd", partition_cols=["reading_date"], 
                           table_properties={"pipelines.autoOptimize.zOrderCols":"reading_hour, reading_timestamp, meter_id", 
                                             "delta.targetFileSize":"128mb"})

dlt.apply_changes(
    flow_name="scd_source_1",
    target="iot_scd",  
    source="source_1",  
    keys=["reading_date", "reading_hour", "reading_timestamp", "meter_id"],  
    sequence_by=f.col("arrival_timestamp"),  
    ignore_null_updates=True
)


dlt.apply_changes(
    flow_name="scd_source_2",
    target="iot_scd",  
    source="source_2",  
    keys=["reading_date", "reading_hour", "reading_timestamp", "meter_id"],  
    sequence_by=f.col("arrival_timestamp"),  
    ignore_null_updates=True
)

dlt.apply_changes(
    flow_name="scd_source_3",
    target="iot_scd", 
    source="source_3",  
    keys=["reading_date", "reading_hour", "reading_timestamp", "meter_id"],  
    sequence_by=f.col("arrival_timestamp"), 
    ignore_null_updates=True
)

# COMMAND ----------

@dlt.view(name="voltage_scd")
def meter_voltage_dlt():
    df = spark.read.table("sadra.hq.meter_voltage")
    return df

# COMMAND ----------

@dlt.table(name="topology", 
           table_properties={"pipelines.autoOptimize.zOrderCols":"meter_id", "delta.targetFileSize":"128mb"})
def meter_topology_dlt():
    df = spark.read.table("sadra.hq.meter_topology")
    df = df.withColumn("starting_date", f.col("starting_timestamp").cast("date"))
    df = df.withColumn("ending_date", f.col("ending_timestamp").cast("date"))
    return df

dlt.create_streaming_table(name="topology_scd", 
                           table_properties={"pipelines.autoOptimize.zOrderCols":"meter_id, starting_date, ending_date"})
dlt.apply_changes_from_snapshot(
    target="topology_scd",
    source="topology",
    keys=["meter_id", "starting_date", "ending_date", "starting_timestamp", "ending_timestamp"])


# COMMAND ----------

@dlt.table(name="iot_scd_joined", partition_cols=["reading_date"], 
           table_properties={"pipelines.autoOptimize.zOrderCols":"reading_hour, reading_timestamp, meter_id, region"})
def iot_data_joined():
    df = dlt.read("iot_scd")
    voltage_multiplier = dlt.read("voltage_scd")
    df = df.join(voltage_multiplier, (df.meter_id == voltage_multiplier.meter_id) & 
                 (df.reading_date == voltage_multiplier.reading_date) , "left_outer")\
        .drop(*[voltage_multiplier.meter_id, voltage_multiplier.reading_date])
    df = df.withColumn("reading_value_multiplier", f.col("reading_value") * f.col("voltage_multiplier"))
    df = df.withColumn("reading_value_3", f.col("reading_value") * 3)
    topology = dlt.read("topology_scd")
    df = df.join(topology, (df.meter_id == topology.meter_id) & \
            (df.reading_date >= topology.starting_date) & \
            (df.reading_date <= topology.ending_date) & \
            (df.reading_timestamp >= topology.starting_timestamp) & \
            (df.reading_timestamp < topology.ending_timestamp), \
            "left_outer").drop(*[topology.meter_id, topology.starting_timestamp, topology.ending_timestamp, topology.starting_date, topology.ending_date])

    return df

@dlt.table(name="iot_hour", partition_cols=["reading_date"], table_properties={"pipelines.autoOptimize.zOrderCols":"reading_hour, meter_id"})
def iot_hour():
    df = dlt.read("iot_scd_joined")
    df = df.groupBy("reading_date", "meter_id", "reading_hour").agg(f.sum("reading_value").alias("reading_value_sum_hour"), 
                                                                    f.sum("reading_value_multiplier").alias("reading_value_multiplier_sum_hour"))
    return df

@dlt.table(name="iot_day", partition_cols=["reading_date"], table_properties={"pipelines.autoOptimize.zOrderCols": "meter_id"})
def iot_day():
    df = dlt.read("iot_scd_joined")
    df = df.groupBy("reading_date", "meter_id").agg(f.sum("reading_value").alias("reading_value_sum_day"),
                                                    f.sum("reading_value_multiplier").alias("reading_value_multiplier_sum_day"))
    return df

@dlt.table(name="iot_region", partition_cols=["reading_date"], table_properties={"pipelines.autoOptimize.zOrderCols": "reading_timestamp, region"})
def iot_region():
    df = dlt.read("iot_scd_joined")
    df = df.groupBy("reading_date", "reading_timestamp", "region").agg(f.sum("reading_value").alias("reading_value_sum_region"),
                                                       f.sum("reading_value_multiplier").alias("reading_value_multiplier_sum_region"))
    return df

