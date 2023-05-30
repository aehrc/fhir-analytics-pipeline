# Databricks notebook source
"""
Demonstrates how to use Spark python dataset API 
and fhirpath with the Pathling `extract()` operation
to navigate nested data types such as `item` in `Questionnaire`.
"""

from pathling import PathlingContext
from pathling import Expression as exp
from pyspark.sql.functions import col, explode, explode_outer, first

#
# Initialize Pathling context that allows up to 5 nesting levels for
# recursive data types.
#
pc = PathlingContext.create(spark, max_nesting_level=5)

#
# Create a FHIR data sourceo on a direcotory with ndjson files with sample `Questionnaire` 
# and `QuestionnaireResponse` resources.
#
fhir_ds = pc.read.ndjson("s3://pathling-demo/staging/devdays/questionnaire")

# COMMAND ----------

#
# Progresively unnnest items of each of the questionnaires, 
# to create the outline of each using their `item.linkId` elements.
# We expect the schema of:
# 
# Questionnaire: struct
#    id: string
#    item: array[struct]                    // level 0
#       linkid: string
#       text: string
#       item: array[struct]                 // level 1
#           linkid: string
#           text: string
#           item: array[struct]             // level 2
#               linkid: string
#               text: string
#               item: array[struct]         // level 3
#                   linkid: string
#                   text: string
#                   item: array[struct]     // level 4
#                       linkid: string
#                       text: string
#                       item: array[struct] // level 5 (no item here)
#                           linkid: string
#                           text: string

questionnarie_structure_df = fhir_ds.extract('Questionnaire', 
    [
        'id',
        exp("item.linkId").alias("linkId_0"), 
        exp("item.item.linkId").alias("linkId_1"), 
        exp("item.item.item.linkId").alias("linkId_2"), 
        exp("item.item.item.item.linkId").alias("linkId_3"),
        exp("item.item.item.item.item.linkId").alias("linkId_4"),
        exp("item.item.item.item.item.item.linkId").alias("linkId_5"),
    ]
)    
    
display(questionnarie_structure_df)

# COMMAND ----------

#
# Extract selected responses to the `f201` questionnaire using 
# fhirpath and the `extract()` operation.
#
# The selected response items:
# - linkid: 2.1 -> gender
# - linkid: 2.3 -> country_of_birth
# - linkid: 2.4 -> marital_status
# - linkid: 3.1 -> smoker
# 
# NOTE: subject.reference can be used to join the responses with Patient resource.
#  
f201_responses_df = fhir_ds.extract('QuestionnaireResponse', 
    [
        'id',
        exp("subject.reference").alias("subject"),
        exp("item.item.where(linkId='2.1').answer.valueString.first()").alias("gender"), 
        exp("item.item.where(linkId='2.3').answer.valueString.first()").alias("country_of_birth"), 
        exp("item.item.where(linkId='2.4').answer.valueString.first()").alias("maritial_status"), 
        exp("item.item.where(linkId='3.1').answer.valueString.first()").alias("smoker"), 
    ], 
    filters=["id = 'f201'"])
    
display(f201_responses_df)

# COMMAND ----------

# 
# Obtain the data frame for `Questionnaire` resource.
#     
questionnaire_df = fhir_ds.read('Questionnaire')

#
# Progresively unnnest items of each of the questionnaires, 
# to create the outline of each using their `item.linkId` elements.
# 

# NOTE: We are using pyspark python dataframe API
# but the same can be achieved in SQL with subqueries.
#

item_df = questionnaire_df \
    .withColumn('item0', explode_outer('item')) \
    .withColumn('item1', explode_outer('item0.item')) \
    .withColumn('item2', explode_outer('item1.item')) \
    .withColumn('item3', explode_outer('item2.item')) \
    .withColumn('item4', explode_outer('item3.item')) \
    .withColumn('item5', explode_outer('item4.item')) \
    .select(col('id'),
            col('item0.linkid').alias('linkid_0'), col('item0.text').alias('text_0'),
            col('item1.linkid').alias('linkid_1'), col('item1.text').alias('text_1'), 
            col('item2.linkid').alias('linkid_2'), col('item3.linkid').alias('linkid_3'),
            col('item4.linkid').alias('linkid_4'), col('item5.linkid').alias('linkid_5')
    )
display(item_df) 

# COMMAND ----------

#
# Extract selected responses to the `f201` questionnaire using 
# SQL (pyspark dataset API).
#
# The selected response items:
# - linkid: 2.1 -> gender
# - linkid: 2.3 -> country_of_birth
# - linkid: 2.4 -> marital_status
# - linkid: 3.1 -> smoker
# 
# NOTE: subject.reference can be used to join the responses with Patient resource.
# 

# 
# Obtain the data frame for `Questionnaire` resource.
#  
response_df = fhir_ds.read('QuestionnaireResponse')


#
# Unnest the responses and project required values
#
response_item_df = response_df \
    .where(col('id') == 'f201') \
    .withColumn('item0', explode_outer('item')) \
    .withColumn('item1', explode_outer('item0.item')) \
    .withColumn('item2', explode_outer('item1.item')) \
    .select(col('id'), col('subject.reference').alias('subject'), 
            col('item1.linkid').alias('linkid_1'), 
            col('item1.answer.valueString').getItem(0).alias('answer_1'))
display(response_item_df) 

# COMMAND ----------

#
# Use `pivot()` rearrange the data frame to the required form. 
#
f201_responses_df = response_item_df.groupBy(col('id'), col('subject')) \
    .pivot("linkid_1", ['2.1', '2.3', '2.4', '3.1']).agg(first('answer_1').alias('answer')) \
    .withColumnRenamed('2.1', 'gender') \
    .withColumnRenamed('2.3', 'country_of_birth') \
    .withColumnRenamed('2.4', 'marital_status') \
    .withColumnRenamed('3.1', 'smoker') 
display(f201_responses_df)
