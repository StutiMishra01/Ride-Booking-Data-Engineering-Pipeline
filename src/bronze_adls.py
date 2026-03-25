# Databricks notebook source
import pandas as pd

files = [
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"bulk_rides"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:
    url = f"<url>"

    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)

    df_spark.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"uber.bronze.{file['file']}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.st_rides

# COMMAND ----------

import pandas as pd

df = pd.read_json("<url>")

dfn = spark.createDataFrame(df)

display(dfn)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.map_cities