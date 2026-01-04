# Databricks notebook source
# MAGIC %md
# MAGIC #  Gold Layer — Dim Sellers
# MAGIC
# MAGIC ## Grain
# MAGIC - One row per seller (`seller_id`)
# MAGIC
# MAGIC ## Description
# MAGIC - Seller master dimension for seller-level analysis
# MAGIC
# MAGIC ## Attributes
# MAGIC - Geographic reference (`geo_id`)
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

from pyspark.sql import functions as F

configure_adls_auth()


# COMMAND ----------

sellers = spark.table("rg_olist_ne_databricks.silver.sellers")
geo     = spark.table("rg_olist_ne_databricks.gold.dim_geolocation")


# COMMAND ----------


dim_sellers = (
    sellers
        .join(
            geo,
            (sellers.seller_city == geo.geolocation_city) &
            (sellers.seller_state == geo.geolocation_state),
            "left"
        )
        .groupBy("seller_id")              
        .agg(
            F.max("geo_id").alias("geo_id") 
        )
        .withColumn("gold_load_timestamp", F.current_timestamp())
)

# COMMAND ----------

(
    dim_sellers
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.dim_sellers")
)

print("Gold dim_sellers written successfully.")


