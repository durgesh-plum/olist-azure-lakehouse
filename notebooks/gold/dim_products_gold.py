# Databricks notebook source
# MAGIC %md
# MAGIC #  Gold Layer — Dim Products
# MAGIC
# MAGIC ## Grain
# MAGIC - One row per product (`product_id`)
# MAGIC
# MAGIC ## Description
# MAGIC - Product master dimension for product-level analysis
# MAGIC
# MAGIC ## Attributes
# MAGIC - Product category
# MAGIC - Physical characteristics
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

from pyspark.sql import functions as F

configure_adls_auth()


# COMMAND ----------

products = spark.read.format("delta").load(silver_products_path)


# COMMAND ----------

dim_products = products.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
)


# COMMAND ----------

(
    dim_products
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.dim_products")
)

print("Gold dim_products written successfully.")

