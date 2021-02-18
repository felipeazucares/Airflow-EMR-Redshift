# Takes the GlobalLandTemperaturesByState.csv data file,
# generates months dtaa for missing months and stores it as a parquet file
# Philip Suggars
# February 2021
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month
from pyspark.sql.types import StructType as R, StructField as Fld, StringType as Str, DateType as Dt, FloatType as Ft
from functools import reduce
from pyspark.sql import DataFrame
import logging

INPUT_FILE = "GlobalLandTemperaturesByState.csv"
OUTPUT_FILE = "fact_temperature_state"
HDFS_INPUT = "hdfs:///user/hadoop/i94"
HDFS_OUTPUT = "hdfs:///user/hadoop/analytics"
STATE_FILE = "dimension_state"


# helper functions


def append_datasets(*datasets):
    return reduce(DataFrame.unionAll, datasets)


def create_spark_session():
    """ Create spark session and return """
    logging.info("Creating spark session")
    spark = (SparkSession.builder.
             config("spark.jars.packages").
             enableHiveSupport().getOrCreate())
    return spark


def read_temperature_data(spark, filename):
    """ Read temperature by state data file into spark dataframe """

    logging.info("Reading temperature data from {}".format(filename))
    temperature_state_schema = R([
        Fld("dt", Dt()),
        Fld("AverageTemperature", Ft()),
        Fld("AverageTemperatureUncertainty", Ft()),
        Fld("State", Str()),
        Fld("Country", Str())
    ])

    # Firstly get the temperature data by city
    df_temperature_by_state = spark.read.options(Header=True).csv(
        filename, temperature_state_schema)
    return df_temperature_by_state


def prepare_temperature_by_state(df_temperature_by_state):
    """ Extract month and year columns for date information """
    # add in month & year to dataset
    logging.info("Prep temperature data")
    df_selected_temperature_by_state = df_temperature_by_state.select(
        "dt", "AverageTemperature", "State", "Country")
    df_selected_temperature_by_state_year = df_selected_temperature_by_state.withColumn(
        'Year', year(df_temperature_by_state.dt))
    df_prepped_temperature_by_state = df_selected_temperature_by_state_year.withColumn(
        'Month', month(df_temperature_by_state.dt))
    return df_prepped_temperature_by_state


def generate_missing_temperature_by_state(df_temperature_by_state):
    """ Generate missing data for months 10-12 by averaging previous 3 years months 10-12 """

    # Generate data for the missing three months of the year 2013 as we only have data from Jan-Sept
    # To do this we'll average Oct-Dec for 2010-2012
    # First extract the months and year we want from the temperature data
    logging.info("Generating missing temperature data")
    df_temp_to_average = df_temperature_by_state.filter((df_temperature_by_state["Country"] == "United States") &
                                                        ((df_temperature_by_state["Year"] == 2010) | (
                                                            df_temperature_by_state["Year"] == 2011) |
                                                         (df_temperature_by_state["Year"] == 2012)) & (
        (df_temperature_by_state["Month"] == 10) |
        (df_temperature_by_state["Month"] == 11) |
        (df_temperature_by_state["Month"] == 12)))

    # Average out the temperatures over the last three years to give a single reading for months 10-12
    df_temp_to_average = df_temp_to_average.groupBy(
        "State", "Month").avg("AverageTemperature")

    # Add in a 2013 year column, reorder the columns and rename the average avg(AverageTemperature)
    df_temp_to_average = df_temp_to_average \
        .withColumn('Year', F.lit(2013)) \
        .select("State", "avg(AverageTemperature)", "Month", "Year") \
        .withColumnRenamed("avg(AverageTemperature)", "AverageTemperature")

    # Now with the original dataset, keep the items for 2013 and columns we want
    df_temperature = df_temperature_by_state \
        .select(F.col("State").alias("state_name"), F.col("AverageTemperature").alias("average_temperature"),
                F.col("Month").alias("month"), F.col("Year").alias("year")) \
        .filter((df_temperature_by_state["year"] == 2013) & (df_temperature_by_state["Country"] == "United States"))

    # Now union the two dataframes together and sort by city and month (year is the same across the dataset - 2013)
    df_fact_temperature_by_state_name = append_datasets(df_temperature, df_temp_to_average) \
        .sort("state_name", "month")
    return df_fact_temperature_by_state_name


def get_state_key_for_temperature_data(df_fact_temperature_by_state_name, df_dimension_state_table):
    """ Join the temperature data to the state dimension table to get the state_key which is not included in the
    temperature data """
    logging.info("Generating state_key for temperature data")
    df_fact_temperature_by_state_key = df_fact_temperature_by_state_name \
        .join(df_dimension_state_table,
              df_fact_temperature_by_state_name.state_name == df_dimension_state_table.state_name, "inner") \
        .select("state_key", F.round("average_temperature", 2).alias("average_temperature"), "month")
    return df_fact_temperature_by_state_key


def read_dimension_state_table(filename):
    """ Read the dimension state table """
    logging.info("Reading state information: {}".format(filename))
    df_dimension_state_table = spark.read.parquet(filename)
    return df_dimension_state_table


def write_parquet(dataset, output_file):
    logging.info("Writing state information: {}".format(output_file))
    """ Output provided dataset to parquet file for use later """
    dataset.write.parquet(output_file)


def main():
    """ Main Routine """


# create spark session
spark = create_spark_session()
# Read the city demographic datafile csv
df_temperature_by_state = read_temperature_data(
    spark, HDFS_INPUT + '/' + INPUT_FILE)
# Select the columns we want and generate month and year columns
df_prepped_temperature_by_state = prepare_temperature_by_state(
    df_temperature_by_state)
# Generate missing data by averaging months in previous years
df_all_temperature_by_state = generate_missing_temperature_by_state(
    df_prepped_temperature_by_state)
# Now read in the Dimension_state_table as we need a state_key for the temperature information and we only have a name
df_dimension_state_table = read_dimension_state_table(
    HDFS_OUTPUT + '/' + STATE_FILE)
# Join the temperature data to the state table so we can extract the state_key
df_all_temperature_by_state_key = get_state_key_for_temperature_data(
    df_all_temperature_by_state, df_dimension_state_table)

df_all_temperature_by_state_key.show()
# Store the result in a parquet file as we will need it later to join with other fact table information
write_parquet(df_all_temperature_by_state_key,
              HDFS_OUTPUT + '/' + OUTPUT_FILE)
