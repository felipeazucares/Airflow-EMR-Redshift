# Takes the GlobalLandTemperaturesByState.csv data file, generates months dtaa for missing months and stores it as a parquet file
# Philip Suggars
# February 202
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, LongType as Lng, TimestampType as Tms, DateType as Dt, FloatType as Ft
from functools import reduce
from pyspark.sql import DataFrame

INPUT_FILE = "GlobalLandTemperaturesByState.csv"
OUTPUT_FILE = "fact_temperature_state"
HDFS_INPUT = "hdfs:///user/hadoop/i94"
HDFS_OUTPUT = "hdfs:///user/hadoop/analytics"

# helper functions


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)


def create_spark_session():
    """ create spark session and return """

    spark = (SparkSession.builder.
             config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2").
             enableHiveSupport().getOrCreate())
    return spark


def read_temperature_data(spark, filename):
    """ Read temperature by state data file into spark dataframe """

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
    df_temperature_by_state = df_temperature_by_state.select(
        "dt", "AverageTemperature", "State", "Country")
    df_temperature_by_state = df_temperature_by_state.withColumn(
        'Year', year(df_temperature_by_state.dt))
    df_temperature_by_state = df_temperature_by_state.withColumn(
        'Month', month(df_temperature_by_state.dt))


def generate_missing_temperature_by_state(df_temperature_by_state):
    """ Generate missing data for months 10-12 by averaging previous 3 years months 10-12 """

    # Generate data for the missing three months of the year 2013 as we only have data from Jan-Sept
    # To do this we'll average Oct-Dec for 2010-2012
    # First extract the months and yeasr we want from the temperature data
    df_temp_to_average = df_temperature_by_state.filter((df_temperature_by_state["Country"] == "United States") & ((df_temperature_by_state["Year"] == 2010) | (df_temperature_by_state["Year"] == 2011) | (
        df_temperature_by_state["Year"] == 2012)) & ((df_temperature_by_state["Month"] == 10) | (df_temperature_by_state["Month"] == 11) | (df_temperature_by_state["Month"] == 12)))

    # average out the temperatures over the last three years
    df_temp_to_average = df_temp_to_average.groupBy(
        "State", "Month").avg("AverageTemperature")

    # add in a 2013 year column, reorder the columns and rename the average avg(AverageTemperature)
    df_temp_to_average = df_temp_to_average \
        .withColumn('Year', F.lit(2013)) \
        .select("State", "avg(AverageTemperature)", "Month", "Year") \
        .withColumnRenamed("avg(AverageTemperature)", "AverageTemperature")

    # filter out the items for 2013 and just keep the columns we want
    df_temperature = df_temperature_by_state \
        .select(F.col("State").alias("state_name"), F.col("AverageTemperature").alias("average_temperature"), F.col("Month").alias("month"), F.col("Year").alias("year")) \
        .filter((df_temperature_by_state["year"] == 2013) & (df_temperature_by_state["Country"] == "United States"))

    # Now union the two dataframes together and sort by city and month (year is the same across the dataset - 2013)
    df_fact_temperature_by_state_name = unionAll(df_temperature, df_temp_to_average) \
        .sort("state_name", "month")
    return df_fact_temperature_by_state_name


def write_parquet(dataset, output_file):
    """ output provided dataset to parquet file for use later """
    dataset.write.mode("overwrite").parquet(output_file)


def main():
    """ Main Routine """


# create spark session
spark = create_spark_session()
# read the city demographic datafile csv
df_temperature_by_state = read_temperature_data(
    spark, HDFS_INPUT + '/' + INPUT_FILE)
df_prepped_temperature_by_state = prepare_temperature_by_state(
    df_temperature_by_state)
df_all_temperature_by_state = generate_missing_temperature_by_state(
    df_prepped_temperature_by_state)
df_all_temperature_by_state.show()
write_parquet(df_all_temperature_by_state,
              HDFS_OUTPUT + '/' + OUTPUT_FILE)
