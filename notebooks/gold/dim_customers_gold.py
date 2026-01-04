# Databricks notebook source
# MAGIC %md
# MAGIC #  Gold Layer — Dim Customers
# MAGIC
# MAGIC ## Grain
# MAGIC - One row per customer (`customer_id`)
# MAGIC
# MAGIC ## Description
# MAGIC - Customer master dimension used for customer-level analytics
# MAGIC
# MAGIC ## Attributes
# MAGIC - Customer identifiers
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

customers = spark.table("silver.customers")


# COMMAND ----------


dim_customers = dim_customers = (
    customers
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        )
        .withColumn(
            "customer_key",
            F.sha2(
                F.concat_ws("|", F.col("customer_unique_id")),
                256
            )
        )
)

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws
dim_customers = (
    dim_customers
    .withColumn(
        "customer_key",
        sha2(concat_ws("||", F.col("customer_unique_id")), 256)
    )
)

# COMMAND ----------

assert dim_customers.filter(F.col("customer_id").isNull()).count() == 0, \
    "Null customer_id found"

assert dim_customers.filter(F.col("customer_unique_id").isNull()).count() == 0, \
    "Null customer_unique_id found"

assert dim_customers.filter(F.col("customer_key").isNull()).count() == 0, \
    "Null customer_key found"


assert (
    dim_customers.count()
    == dim_customers.select("customer_unique_id").distinct().count()
), "Duplicate customer_unique_id detected in dim_customers"


# COMMAND ----------

(
    dim_customers
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.dim_customers")
)

print("Gold dim_customers written successfully.")


