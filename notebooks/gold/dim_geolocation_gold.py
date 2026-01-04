# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Dim Geolocation
# MAGIC
# MAGIC ## Grain
# MAGIC - One row per geolocation
# MAGIC
# MAGIC ## Description
# MAGIC - Geographic lookup dimension used by customers and sellers
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------


from pyspark.sql import functions as F
configure_adls_auth()

# COMMAND ----------

geo = spark.read.format("delta").load(silver_geolocation_path)

dim_geolocation = geo.select(
    "geo_id",
    "geolocation_city",
    "geolocation_state",
    "geolocation_lat",
    "geolocation_lng"
)


# COMMAND ----------

(
    dim_geolocation
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.dim_geolocation")
)

print("Gold dim_geolocation written successfully.")

