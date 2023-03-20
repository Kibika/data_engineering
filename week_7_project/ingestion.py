#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sodapy import Socrata
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
client = Socrata("data.cityofnewyork.us", None)

results = client.get("h9gi-nx95")

# creating the session
spark = SparkSession.builder.getOrCreate()

# create spark dataframe
df = spark.createDataFrame(results)

# write parquet file to gcs bucket
df.write.format("parquet").option("path", f"gs://de-zoomcamp-375510-cf05a71f2b3e/pq/nyc").save(header=True)

