
# Takes the us-cities-demographic.csv data file, summarises it by state and stores it as a parquet file
# Philip Suggars
# 2021

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

INPUT_FILE = "us-cities-demographics.csv"
OUTPUT_FILE = "dim_state"
HDFS_INPUT = "hdfs:///user/hadoop/i94"
HDFS_OUTPUT = "hdfs:///user/hadoop/analytics"


def create_spark_session(AWS_ACCESS_KEY, AWS_SECRET_KEY):
    """ create spark session and return """

    spark = (SparkSession.builder.
             config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2").
             enableHiveSupport().getOrCreate())
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    #hadoop_conf.set("fs.s3a.endpoint", "us-west-2.amazonaws.com")
    return spark


def read_city_demographic_data(spark, filename):
    """ read named city data file into spark dataframe """

    demographic_schema = R([
        Fld("City", Str()),
        Fld("State", Str()),
        Fld("Median Age", Ft()),
        Fld("Male Population", Int()),
        Fld("Female Population", Int()),
        Fld("Total Population", Int()),
        Fld("Number of Veterans", Int()),
        Fld("Foreign Born", Int()),
        Fld("Average Household Size", Ft()),
        Fld("State Code", Str()),
        Fld("Race", Str()),
        Fld("Count", Int())
    ])

    df_demographic = spark.read.options(Header=True, Delimiter=";").csv(
        filename, demographic_schema)

    return df_demographic


def aggregate_city_demographc_data(df_demographic):
    """ take city demographic data and aggregate up to state level - return aggregate state aggregate dataframe """
    # the demographic data has 4 entires per city based on ethnic breakdown - we only want the city->state relationships so select distinct cities
    df_demographic = df_demographic.dropDuplicates(["City"])

    # Now we need to calcuate the average details by state  in each state
    df_demo_by_state = df_demographic.groupBy("State", "State Code") \
        .agg(F.avg("Median Age").alias("average_age"),
             F.sum("Female Population").alias("female_urban_population"),
             F.sum("Male Population").alias("male_urban_population"),
             F.sum("Total Population").alias("total_urban_population")) \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("State", "state_name") \
        .sort("State")

    # create dimension table for non time variant values
    df_dimension_state_table = df_demo_by_state \
        .select(F.col("state_code").alias("state_key"), "state_name", "average_age", "female_urban_population", "male_urban_population", "total_urban_population") \
        .dropDuplicates(["state_key"]) \
        .sort("state_key")
    df_dimension_state_table.show(20)
    return df_dimension_state_table


def write_parquet(dataset, partition, output_file):
    """ output provided dataset to parquet file for use later """
    # write the non variant dimension data out to a parquet file - state dimension table
    dataset.write.mode("overwrite").parquet(output_file)


def main():
    """ Main Routine """


# read key details from dl.cfg file
config = configparser.ConfigParser()
config.read("dl.cfg")
# set os variables from config file
AWS_ACCESS_KEY_ID = config.get("AWS", "AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get(
    "AWS", "AWS_SECRET_ACCESS_KEY")

# create spark session
spark = create_spark_session(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
# read the city demographic datafile csv
df_demographic_data = read_city_demographic_data(
    spark, HDFS_INPUT + '/' + INPUT_FILE)
df_dimension_state_table = aggregate_city_demographc_data(df_demographic_data)
write_parquet(df_dimension_state_table, "state_code",
              HDFS_OUTPUT + '/' + OUTPUT_FILE)