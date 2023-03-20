import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


# credentials = '/home/stl/.gc/second-chariot-375510-9c78077d91fd.json'
conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('test') \
        .set("spark.jars", "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.9/gcs-connector-hadoop3-2.2.9-shaded.jar") \
        # .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        # .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-696548011909-iyzb4jqm')


df = spark.read.parquet('gs://de-zoomcamp-375510-cf05a71f2b3e/pq/nyc/*')

df.write.format("bigquery").option("table", "second-chariot-375510.nyc.mv_crashes").mode("overwrite").save(header=True)

