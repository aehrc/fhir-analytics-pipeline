# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Number of non vaccinated patients startified by risk score grouped by area code
# MAGIC
# MAGIC -  `risk_score = hasBMIOver30 + 3 * hadCKD  + 2 * hasHD`
# MAGIC -  `area_code = first three digits of postalCode`
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC USE devdays_sql;
# MAGIC SELECT 
# MAGIC   substring(postalCode,1,3) area_code, 
# MAGIC   (cast(hasBMIOver30 AS INT) + 3 * cast(hasCKD AS INT) + 2 * cast(hasHD AS INT)) AS risk_score
# MAGIC FROM covid19_view 
# MAGIC WHERE NOT isCovidVaccinated AND postalCode IS NOT NULL; 
