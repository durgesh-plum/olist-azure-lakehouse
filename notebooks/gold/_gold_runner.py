# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Runner
# MAGIC
# MAGIC ## Purpose
# MAGIC - Orchestrates creation of all Gold-layer fact and dimension tables
# MAGIC - Enforces dimensional modeling standards (star schema)
# MAGIC - Produces analytics-ready datasets for BI consumption
# MAGIC
# MAGIC ## Execution
# MAGIC - Triggered by Azure Data Factory
# MAGIC - Runs only after Silver layer completes successfully
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# MAGIC %run "./dim_date_gold"

# COMMAND ----------

# MAGIC %run "./dim_customers_gold"
# MAGIC

# COMMAND ----------

# MAGIC %run "./dim_products_gold"
# MAGIC

# COMMAND ----------

# MAGIC %run "./dim_sellers_gold"
# MAGIC

# COMMAND ----------

# MAGIC %run "./fact_orders_gold"
# MAGIC

# COMMAND ----------

# MAGIC %run "./fact_order_items_gold"
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

configure_adls_auth()

# COMMAND ----------

print("✅ Gold layer completed successfully")
