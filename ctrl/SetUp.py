# Databricks notebook source
"""
Sets up the the workspace for the `fhir-analytics-pipeline` demo
"""

# COMMAND ----------

#
# Download test data to DBFS
# 

DATA_DIR_URL = 'https://aehrc.github.io/fhir-analytics-pipeline/data/ndjson'
DATA_FILES = ['Patient.ndjson', 'Questionnaire.ndjson', 'QuestionnaireResponse.ndjson']
DBFS_DATA_DIR_URL = 'dbfs:/tmp/DevDays/data'

import urllib
from os import path
from tempfile import TemporaryDirectory

dbutils.fs.rm(DBFS_DATA_DIR_URL, True)
with TemporaryDirectory() as tmpdirname:
    for filename in DATA_FILES:
        input_url = path.join(DATA_DIR_URL, filename)
        output_path = path.join(tmpdirname, filename)
        urllib.request.urlretrieve(input_url, output_path)
        temp_url= 'file://' + output_path
        output_url = path.join(DBFS_DATA_DIR_URL,filename)
        print(f"Downloading `{input_url}` to: `{output_url}`")
        dbutils.fs.mv(temp_url, output_url)
