# Simple routine to check that there are no nulls in the key or data fields
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging


HDFS_OUTPUT = "hdfs:///user/hadoop/analytics"
INPUT_FILE = "fact_arrivals_by_state_month"


def read_parquet_file(spark, filename):
    """ Read the named parquet file and return it as a dataframe """
    logging.info("Reading parquet data:{}".format(filename))
    df_input = spark.read.parquet(filename)
    return df_input


def null_checker(dataset, column_name):
    """ Return the count of nulls in the dataframe provided"""
    logging.info("Checking for nulls :{} {}".format(dataset, column_name))
    return dataset.filter(F.col(column_name).isNull()).count()


def create_spark_session():
    """ Create spark session and return """
    logging.info("Creating spark session")
    spark = (SparkSession.builder.
             enableHiveSupport().getOrCreate())
    return spark


def main():
    """ Main routine """
    # Create spark session
    spark = create_spark_session()
    # Read the fact table
    fact_table_arrivals = read_parquet_file(
        spark, HDFS_OUTPUT + '/' + INPUT_FILE)
    # Create a list of all the fields in the fact table we want to check
    column_name = ['state_key', 'month', 'year',
                   'average_age', 'average_temperature']
    # Now check each one has no nulls

    for item in column_name:
        if (null_checker(fact_table_arrivals, item)) > 0:
            print("Nulls found for {}".format(item))
            raise Exception("Data quality test failed for {}".format(item))
        else:
            print("{} has passed null check".format(item))
