# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS uber.bronze.st_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.silver_obt

# COMMAND ----------

df = spark.read.table("uber.bronze.rides_raw")
df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema)).select("parsed_rides.*")
display(df_parsed)

# COMMAND ----------

display(spark.sql("select * from uber.bronze.st_rides"))

# COMMAND ----------

pip install Jinja2

# COMMAND ----------

import pandas as pd
df = pd.read_json("<url>")
dfn = spark.createDataFrame(df)
display(dfn)

# COMMAND ----------

jinja_config = [
    {
        "table" : "uber.bronze.st_rides st_rides",
        "select" : 'st_rides.ride_id, st_rides.confirmation_number, st_rides.passenger_id, st_rides.driver_id, st_rides.vehicle_id, st_rides.pickup_location_id, st_rides.dropoff_location_id, st_rides.vehicle_type_id, st_rides.vehicle_make_id, st_rides.payment_method_id, st_rides.ride_status_id, st_rides.pickup_city_id, st_rides.dropoff_city_id, st_rides.cancellation_reason_id, st_rides.passenger_name, st_rides.passenger_email, st_rides.passenger_phone, st_rides.driver_name, st_rides.driver_rating, st_rides.driver_phone, st_rides.driver_license, st_rides.vehicle_model, st_rides.vehicle_color, st_rides.license_plate, st_rides.pickup_address, st_rides.pickup_latitude, st_rides.pickup_longitude, st_rides.dropoff_address, st_rides.dropoff_latitude, st_rides.dropoff_longitude, st_rides.distance_miles, st_rides.duration_minutes, st_rides.booking_timestamp, st_rides.pickup_timestamp, st_rides.dropoff_timestamp, st_rides.base_fare, st_rides.distance_fare, st_rides.time_fare, st_rides.surge_multiplier, st_rides.subtotal, st_rides.tip_amount, st_rides.total_fare, st_rides.rating',
        "where" : ""
    },
    {
        "table" : "uber.bronze.map_vehicle_makes map_vehicle_makes",
        "select" : "map_vehicle_makes.vehicle_make",
        "where" : "",
        "on" : "st_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table" : "uber.bronze.map_vehicle_types map_vehicle_types",
        "select" : "map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute",
        "where" : "",
        "on" : "st_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    },
    {
        "table" : "uber.bronze.map_ride_statuses map_ride_statuses",
        "select" : "map_ride_statuses.ride_status",
        "where" : "",
        "on" : "st_rides.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table" : "uber.bronze.map_payment_methods map_payment_methods",
        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where" : "",
        "on" : "st_rides.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table" : "uber.bronze.map_cities map_cities",
        "select" : "map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at",
        "where" : "",
        "on" : "st_rides.pickup_city_id = map_cities.city_id"
    },
    {
        "table" : "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "where" : "",
        "on" : "st_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    }
]

# COMMAND ----------

from jinja2 import Template

jinja_str = """
    SELECT 
        {% for config in jinja_config %}
            {{ config.select }}
                {% if not loop.last %}
                    , 
                {% endif %}
        {% endfor %}
    FROM 
        {% for config in jinja_config %}
            {{ config.table }}
                {% if not loop.last %}
                    , 
                {% endif %}
        {% endfor %}
   
        {% for config in jinja_config %}
            {% if loop.first %}
                {% if config.where != "" %}
                    WHERE
                {% endif %}
            {% endif %}
            {{ config.where }}
                {% if not loop.last %}
                    {% if config.where != "" %}
                        AND 
                    {% endif %}
                {% endif %}
        {% endfor %}

"""

template = Template(jinja_str)
rend_temp = template.render(jinja_config=jinja_config)
print(rend_temp)

# COMMAND ----------

display(spark.sql(rend_temp).count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM uber.bronze.st_rides st 
# MAGIC LEFT JOIN uber.bronze.map_vehicle_types mvt 
# MAGIC ON st.vehicle_type_id = mvt.vehicle_type_id
# MAGIC LEFT JOIN uber.bronze.map_vehicle_makes mvm 
# MAGIC ON st.vehicle_make_id = mvm.vehicle_make_id
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp()