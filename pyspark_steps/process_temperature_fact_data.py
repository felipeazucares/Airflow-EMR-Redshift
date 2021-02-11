
from pyspark.sql import SparkSession
import os
import configparser
import pandas as pd
import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, LongType as Lng, TimestampType as Tms, DateType as Dt, FloatType as Ft
from functools import reduce
from pyspark.sql import DataFrame


# helper functions
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)


spark = (SparkSession.builder.
         config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2").
         enableHiveSupport().getOrCreate())
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
#hadoop_conf.set("fs.s3a.endpoint", "us-west-2.amazonaws.com")

temperature_state_schema = R([
    Fld("dt", Dt()),
    Fld("AverageTemperature", Ft()),
    Fld("AverageTemperatureUncertainty", Ft()),
    Fld("State", Str()),
    Fld("Country", Str())
])

# Firstly get the temperature data by city
df_temperature_state = spark.read.options(Header=True).csv(
    "GlobalLandTemperaturesByState.csv", temperature_state_schema)

df_temperature_state.show(10)
# add in month & year to dataset
df_temperature_state = df_temperature_state.select(
    "dt", "AverageTemperature", "State", "Country")
df_temperature_state = df_temperature_state.withColumn(
    'Year', year(df_temperature_state.dt))
df_temperature = df_temperature_state.withColumn(
    'Month', month(df_temperature_state.dt))

# Generate data for the missing three months of the year 2013 as we only have data from Jan-Sept
# To do this we'll average Oct-Dec for 2010-2012
# First extract the months and yeasr we want from the temperature data
df_temp_to_average = df_temperature.filter((df_temperature["Country"] == "United States") & ((df_temperature["Year"] == 2010) | (df_temperature["Year"] == 2011) | (
    df_temperature["Year"] == 2012)) & ((df_temperature["Month"] == 10) | (df_temperature["Month"] == 11) | (df_temperature["Month"] == 12)))

# average out the temperatures over the last three years
df_temp_to_average = df_temp_to_average.groupBy(
    "State", "Month").avg("AverageTemperature")

# add in a 2013 year column, reorder the columns and rename the average avg(AverageTemperature)
df_temp_to_average = df_temp_to_average \
    .withColumn('Year', F.lit(2013)) \
    .select("State", "avg(AverageTemperature)", "Month", "Year") \
    .withColumnRenamed("avg(AverageTemperature)", "AverageTemperature")

# filter out the items for 2013 and just keep the columns we want
df_temperature = df_temperature \
    .select(F.col("State").alias("state_name"), F.col("AverageTemperature").alias("average_temperature"), F.col("Month").alias("month"), F.col("Year").alias("year")) \
    .filter((df_temperature["year"] == 2013) & (df_temperature["Country"] == "United States"))

# Now union the two dataframes together and sort by city and month (year is the same across the dataset - 2013)
df_fact_temperature_by_state_name = unionAll(df_temperature, df_temp_to_average) \
    .sort("state_name", "month")

# df_fact_temperature_by_state_name.show(36)
