# Databricks notebook source
import dlt
import pyspark.sql.functions as f

dlt.create_streaming_table("iot_streams")

@dlt.append_flow(target="iot_streams")
def iot_source_1():
    return (
        spark.readStream.table("sadra.hq.iot_source_1")
    )

@dlt.append_flow(target="iot_streams")
def iot_source_2():
    return (
        spark.readStream.table("sadra.hq.iot_source_2")
    )

@dlt.append_flow(target="iot_streams")
def iot_source_3():
    return (
        spark.readStream.table("sadra.hq.iot_source_3")
    )

@dlt.view(name="transformed_streams")
def transformed_streams():
    df = dlt.read_stream("iot_streams")
    df = df.withColumn("reading_type_A", f.when(f.col("reading_type") == "A", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_B", f.when(f.col("reading_type") == "B", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_C", f.when(f.col("reading_type") == "C", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_type_D", f.when(f.col("reading_type") == "D", f.col("reading_value")).otherwise(None))
    df = df.withColumn("reading_date", f.col("reading_timestamp").cast("date"))
    df = df.withColumn("reading_hour", f.hour(f.col("reading_timestamp")))
    return df

dlt.create_streaming_table(name="iot_scd", partition_cols=["reading_date"], table_properties={"pipelines.autoOptimize.zOrderCols":"reading_hour, reading_timestamp, meter_id"})
dlt.apply_changes(
    target="iot_scd",  # The customer table being materilized
    source="transformed_streams",  # the incoming CDC
    keys=["reading_date", "reading_hour", "reading_timestamp", "meter_id"],  # what we'll be using to match the rows to upsert
    sequence_by=f.col("arrival_timestamp"),  # we deduplicate by operation date getting the most recent value
    ignore_null_updates=True
)




# COMMAND ----------

@dlt.view(name="voltage_scd")
def meter_voltage_dlt():
    df = spark.read.table("sadra.hq.meter_voltage")
    return df

#dlt.create_streaming_table(name="voltage_scd", partition_cols=["reading_date"], table_properties={"pipelines.autoOptimize.zOrderCols":"meter_id"})
#dlt.apply_changes_from_snapshot(
#    target="voltage_scd",  # The customer table being materilized
#    source="voltage_factor",  # the incoming CDC
#    keys=["reading_date", "meter_id"],  # what we'll be using to match the rows to upsert
#)

#@dlt.table(name="topology", table_properties={"pipelines.autoOptimize.zOrderCols":"reading_date, meter_id"})
#def meter_topology_dlt():
#    df = spark.read.table("sadra.hq.meter_topology")
#    return df

#dlt.create_streaming_table(name="topology_scd", partition_cols=["reading_date"], table_properties={"pipelines.autoOptimize.zOrderCols":"meter_id"})
#dlt.apply_changes_from_snapshot(
#    target="topology_scd",  # The customer table being materilized
#    source="topology",  # the incoming CDC
#    keys=["reading_date", "meter_id"],  # what we'll be using to match the rows to upsert
#)


# COMMAND ----------

@dlt.table(name="topology", table_properties={"pipelines.autoOptimize.zOrderCols":"meter_id"})
def meter_topology_dlt():
    df = spark.read.table("sadra.hq.meter_topology")
    df = df.withColumn("starting_date", f.col("starting_timestamp").cast("date"))
    df = df.withColumn("ending_date", f.col("ending_timestamp").cast("date"))
    return df

dlt.create_streaming_table(name="topology_scd", table_properties={"pipelines.autoOptimize.zOrderCols":"meter_id, starting_date, ending_date"})
dlt.apply_changes_from_snapshot(
    target="topology_scd",  # The customer table being materilized
    source="topology",  # the incoming CDC
    keys=["meter_id", "starting_date", "ending_date", "starting_timestamp", "ending_timestamp"],  # what we'll be using to match the rows to upsert
)


# COMMAND ----------

@dlt.table(name="iot_scd_joined", partition_cols=["reading_date"], table_properties={"pipelines.autoOptimize.zOrderCols":"reading_hour, reading_timestamp, meter_id, region"})
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

