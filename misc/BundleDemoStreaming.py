# Databricks notebook source
"""
Demonstrates how to use Bundles to implement near real time analytics 
with Spark Structure Streaming.
"""

from pyspark.sql.functions import col, count
from pathling import PathlingContext
from pathling.datasink import ImportMode


#
# Create a Pathing context to use for encoding.
#
pc = PathlingContext.create(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP  SCHEMA IF EXISTS devdays_bundles CASCADE;
# MAGIC CREATE SCHEMA IF NOT EXISTS devdays_bundles;
# MAGIC USE devdays_bundles;

# COMMAND ----------

#
# Create a streaming source from a location at S3. 
# Each file with a JSON encoded bundle is converted into a single element in the stream.
# Other sources (e.g. kafka) are supported as well.
# see: https://pathling.csiro.au/docs/libraries/kafka
#
bundles_stream = spark.readStream.text('s3://pathling-demo/staging/devdays/bundles/', wholetext=True)

#
# Use Pathling to convert text stream of bundles to structured resource streams 
#
patient_stream  = pc.encode_bundle(bundles_stream, 'Patient')
condition_stream  = pc.encode_bundle(bundles_stream, 'Condition')

# COMMAND ----------

#
# Run a simple streaming query on the resource stream
#

display(
    patient_stream.groupBy(col('gender')).agg(count("*"))
)

# COMMAND ----------

#
# Run streaming updates to the resource data lake table
#
condition_stream.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/DevDays/checkpoints/Conditions") \
  .toTable("Conditions")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run a simple query to demonstrate near real time updates.
# MAGIC SELECT COUNT(*) FROM conditions;
