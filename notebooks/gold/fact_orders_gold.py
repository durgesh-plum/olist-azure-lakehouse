# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Fact Orders
# MAGIC
# MAGIC ## Grain
# MAGIC - One row per order (`order_id`)
# MAGIC
# MAGIC ## Measures
# MAGIC - order_count (implicit)
# MAGIC - delivery_time_days
# MAGIC - order_to_approval_minutes
# MAGIC
# MAGIC ## Foreign Keys
# MAGIC - customer_key
# MAGIC - order_date_key
# MAGIC - delivery_date_key
# MAGIC
# MAGIC ## Guarantees
# MAGIC - No duplicate orders
# MAGIC - Join-safe to all dimensions
# MAGIC - Timestamp-derived metrics are non-negative
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

from pyspark.sql import functions as F

configure_adls_auth()

# COMMAND ----------

orders = spark.read.format("delta").load(silver_orders_path)
customers = spark.read.format("delta").load(gold_dim_customers_path)
dates = spark.read.format("delta").load(gold_dim_date_path)


# COMMAND ----------

date_lookup = (
    dates
    .select("date", "date_key")
    .dropDuplicates(["date"])
)


# COMMAND ----------

customer_lookup = (
    spark.read.format("delta")
    .load(gold_dim_customers_path)
    .dropDuplicates(["customer_id"])
)


# COMMAND ----------

orders_df = orders.alias("o")
customers_df = customer_lookup.alias("c")

# COMMAND ----------

fact_orders = (
    orders_df
    .join(customers_df, F.col("o.customer_id") == F.col("c.customer_id"), "left")

    .join(
        date_lookup.alias("od"),
        F.col("o.order_purchase_timestamp").cast("date") == F.col("od.date"),
        "left"
    )

    .join(
        date_lookup.alias("dd"),
        F.col("o.order_delivered_customer_date").cast("date") == F.col("dd.date"),
        "left"
    )

    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("c.geo_id").alias("geo_id"),
        F.col("od.date_key").alias("order_date_key"),
        F.col("dd.date_key").alias("delivery_date_key"),
        F.col("o.order_status").alias("order_status"),

        F.datediff(
            F.col("o.order_delivered_customer_date"),
            F.col("o.order_purchase_timestamp")
        ).alias("delivery_time_days"),

        ((F.unix_timestamp(F.col("o.order_approved_at")) -
          F.unix_timestamp(F.col("o.order_purchase_timestamp"))) / 60
        ).alias("order_to_approval_minutes"),

        F.col("o.silver_load_timestamp").alias("silver_load_timestamp")
    )
)


# COMMAND ----------

assert fact_orders.select("order_id").distinct().count() == fact_orders.count(), \
    "DQ failed: Duplicate order_id detected"


# COMMAND ----------

assert fact_orders.filter(
    F.col("order_id").isNull() |
    F.col("customer_id").isNull() |
    F.col("order_date_key").isNull()
).count() == 0, "DQ failed: Null business keys in fact_orders"


# COMMAND ----------

assert fact_orders.filter(
    F.col("delivery_time_days") < 0
).count() == 0, "DQ failed: Negative delivery time"


# COMMAND ----------

(
    fact_orders
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold.fact_orders")

)

print("Gold fact_orders written successfully.")
