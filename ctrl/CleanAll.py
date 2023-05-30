# Databricks notebook source
"""
Removes all resources created by the analytics pipeline demo
"""

# COMMAND ----------

# Clean up DBFS directories
dbutils.fs.rm('/tmp/DevDays', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP all schemas
# MAGIC DROP SCHEMA IF EXISTS devdays_fhir CASCADE;
# MAGIC DROP SCHEMA IF EXISTS devdays_sql CASCADE;
# MAGIC DROP SCHEMA IF EXISTS devdays_bundles CASCADE;
