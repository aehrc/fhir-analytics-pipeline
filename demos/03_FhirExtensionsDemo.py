# Databricks notebook source
"""
Demonstrates how to use FHIRPath with the Pathling `extract()` operation
and SQL to obtain extension values.

Data comprise of ndjson file exported from the 100 patients 
database of https://bulk-data.smarthealthit.org/.

Extensions defined on `Patient` resource:

- string: http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName
- Address: http://hl7.org/fhir/StructureDefinition/patient-birthPlace
- decimal: http://synthetichealth.github.io/synthea/quality-adjusted-life-years 
- decimal: http://synthetichealth.github.io/synthea/disability-adjusted-life-years


Extensions defined on `address` element:

- complex: http://hl7.org/fhir/StructureDefinition/geolocation
    - decimal: latitude
    - decimal: longitude

We demonstrate how select the `city_of_birth`, `quality_adjusted_life_years`, 
`address_latitude`, `address_longitude`  values for each Patient 
using both FHIRpath and SQL.
"""


from pathling import PathlingContext

#
# Initialize Pathling context with enabled support for extensions
#
pc = PathlingContext.create(spark, enable_extensions = True)

#
# Create a FHRI data source on a directory with ndjons encoded resource files.
# 
fhir_ds = pc.read.ndjson('dbfs:/tmp/DevDays/data/')

# COMMAND ----------

from pathling import Expression as exp

#
# Select extension values using `extract()` operation 
# and the FHIRPath `extension()` function.
#

result = fhir_ds.extract('Patient', [
    exp("id"), 
    exp("extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress.city.first()").alias("city_of_birth"),
    exp("extension('http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal").alias("quality_adjusted_life_years"),
    exp("address.extension('http://hl7.org/fhir/StructureDefinition/geolocation').extension('latitude').valueDecimal.first()").alias("address_latitude"),
    exp("address.extension('http://hl7.org/fhir/StructureDefinition/geolocation').extension('longitude').valueDecimal.first()").alias("address_longitude"),
])

display(result.orderBy('id'))

# COMMAND ----------

#
# Obtain the data frame for the `Patient` resource and register it a temporary view
# so that it can be used in SQL queries.
#
fhir_ds.read('Patient').createOrReplaceTempView('patient_with_extensions')

# COMMAND ----------

# MAGIC %sql
# MAGIC --
# MAGIC -- Select extension values using SQL 
# MAGIC -- and direct references to the `_extension` MAP column.
# MAGIC -- 
# MAGIC SELECT id, 
# MAGIC   filter(_extension[_fid], e -> e.url='http://hl7.org/fhir/StructureDefinition/patient-birthPlace').valueAddress[0].city AS city_of_birth,
# MAGIC   filter(_extension[_fid], e -> e.url='http://synthetichealth.github.io/synthea/quality-adjusted-life-years').valueDecimal[0] AS quality_adjusted_life_years, 
# MAGIC   filter(_extension[filter(_extension[address[0]._fid], e -> e.url='http://hl7.org/fhir/StructureDefinition/geolocation')[0]._fid], 
# MAGIC         e -> e.url='latitude')[0].valueDecimal AS address_latitude, 
# MAGIC   filter(_extension[filter(_extension[address[0]._fid], e -> e.url='http://hl7.org/fhir/StructureDefinition/geolocation')[0]._fid], 
# MAGIC         e -> e.url='longitude')[0].valueDecimal AS address_longitude 
# MAGIC FROM patient_with_extensions 
# MAGIC ORDER BY id;
