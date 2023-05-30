# Databricks notebook source
"""
Extract the relevant data for COVID-19 self-service analytics from FHIR resource delta tables 
into the flat table in an SQL database.

:param INPUT_FHIR_SCHEMA: the name of the source database schema with FHIR resource data
:param OUTPUT_SQL_SCHEMA: the name of the database schema to load the extracted data into
"""

dbutils.widgets.text('INPUT_FHIR_SCHEMA', 'devdays_fhir')
dbutils.widgets.text('OUTPUT_SQL_SCHEMA', 'devdays_sql')

INPUT_FHIR_SCHEMA=dbutils.widgets.get('INPUT_FHIR_SCHEMA')
OUTPUT_SQL_SCHEMA=dbutils.widgets.get('OUTPUT_SQL_SCHEMA')

print(f"""Extracting covid-19 analytics view:
 from: `{INPUT_FHIR_SCHEMA}`
 to: `{OUTPUT_SQL_SCHEMA}`
 """)

# COMMAND ----------

from pathling import PathlingContext
from pathling import Expression as exp

# Initialize Pathling context
pc = PathlingContext.create(spark)

# Create a Pathling `DataSource` from the tables of the delta lake schema
fhir_ds = pc.read.tables(INPUT_FHIR_SCHEMA)

# Use the extract() operation to define the covid-19 view
coivd19_view_df = fhir_ds.extract('Patient',
    columns= [
        exp("id"),
        exp("gender"),
        exp("birthDate"),
        exp("address.postalCode.first()").alias("postalCode"),
        exp("reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|56265001))").alias("hasHD"),
        exp("reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|709044004))").alias("hasCKD"),
        exp("reverseResolve(Observation.subject).where(code.subsumedBy(http://loinc.org|39156-5)).exists(valueQuantity > 30 'kg/m2')").alias("hasBMIOver30"),
        exp("reverseResolve(Immunization.patient).vaccineCode.memberOf('https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines').anyTrue()").alias("isCovidVaccinated"),
    ],
    filters = [
        "address.country.first() = 'US'"
    ]
)

# Load the extracted data to the destination schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SQL_SCHEMA}")
sql_view_name = f"{OUTPUT_SQL_SCHEMA}.covid19_view"

coivd19_view_df.write.saveAsTable(sql_view_name, mode='overwrite')

#DEBUG: Display the schema and the data sample from the created view
print(f"Schema and data in: {sql_view_name}:")
sql_view = spark.read.table(sql_view_name)
sql_view.printSchema()
sql_view.show(5)
