# Databricks notebook source
"""
Merge resource data form ndjson files into a delta lake schema.
Encodes the ndjson files as SparkSQL datasets and merges them with the delta tables.

:param SOURCE_URL: the URL to the directory with ndjson encoded resouce files
:param DESTINATION_SCHEMA: the name of the data lake schema to merge the resource data into
"""

dbutils.widgets.text('SOURCE_URL', 'dbfs:/tmp/DevDays/demo-etl')
dbutils.widgets.text('DESTINATION_SCHEMA', 'devdays_fhir')

SOURCE_URL=dbutils.widgets.get('SOURCE_URL')
DESTINATION_SCHEMA=dbutils.widgets.get('DESTINATION_SCHEMA')

print(f"""Loading and merging data:
 from: `{SOURCE_URL}`
 to schema: `{DESTINATION_SCHEMA}`
 """)

# COMMAND ----------

from pathling import PathlingContext
from pathling.datasink import ImportMode

# Initialize Pathling context
pc = PathlingContext.create(spark)

# Load resources data from njdson files. Resource types are infered from file names 
# e.g.: 'Observation.0003.ndjson' -> Observation.
# Creates a Patling `DataSource` instance, which conceptually maps
# resource type to their corresponding Spark data frames.
# So in our case: 
# { 
#   'Patient' -> patient_data_frame,
#   'Condition' -> condition_data_frame,
#   ...
# }
# see: https://pathling.csiro.au/docs/python/pathling.html#pathling.datasource.DataSource
fhir_resources_ds = pc.read.ndjson(SOURCE_URL)

#DEBUG: Show some of the encoded resources
print(f"ndjson data in `{SOURCE_URL}` includes:")
print(" %s patients" % fhir_resources_ds.read('Patient').count())
print(" %s conditions" % fhir_resources_ds.read('Condition').count())
print(" %s observations" % fhir_resources_ds.read('Observation').count())
print(" %s immunizations" % fhir_resources_ds.read('Immunization').count())

# COMMAND ----------

# Create the destination schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DESTINATION_SCHEMA}")

# Merge the FHIR data form the `SOURCE_URL` into it the `DESTINATION_SCHEMA`.
# For each resource in the `fhir_resources_ds` merge its dataframe with the 
# data in the resource table in the schema.
fhir_resources_ds.write.tables(DESTINATION_SCHEMA, ImportMode.MERGE)

#DEBUG: Dislay created/existing tables in the destination schema
print(f"FHIR resource tables in schema `{DESTINATION_SCHEMA}`:")
spark.catalog.setCurrentDatabase(DESTINATION_SCHEMA)
for table in spark.catalog.listTables():
    print(" `%s` with %s rows" % (table.name, spark.read.table(table.name).count()))
