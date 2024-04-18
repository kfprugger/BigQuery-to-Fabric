from pyspark.sql import functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

conf.set("spark.jars.packages", "io.delta:delta-core:1.0.0")
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
)

spark = SparkSession.builder.appName("provider_test").config(conf=conf).getOrCreate()

# read data from Big Query
prescriber_data = (
    spark.read.format("bigquery")
    .option("table", "bigquery-public-data.cms_medicare.part_d_prescriber_2014")
    .load()
)

# filter data -- will add more to simluate a delta load without the filter
prescriber_filtered = prescriber_data.filter(
    "npi  between '1053373522' and '1053374523'"
)

# create report by Taxi driver
prescriber_report = prescriber_filtered

# save as delta_table
(
    prescriber_report.write.mode("append")
    .format("delta")
    .save("gs://az-xplat-medicare/BQHospitalSampleDeltaExport")
)

# print data
prescriber_report.show(20)