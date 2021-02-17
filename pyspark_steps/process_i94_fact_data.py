# Takes the GlobalLandTemperaturesByState.csv data file, generates months dtaa for missing months and stores it as a parquet file
# Philip Suggars
# February 202
from os import listdir
from os.path import isfile, join
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, LongType as Lng, TimestampType as Tms, DateType as Dt, FloatType as Ft
from functools import reduce
from pyspark.sql import DataFrame

INPUT_FILE = "airport-codes_csv.csv"
OUTPUT_FILE = "fact_arrivals_by_state_month"
HDFS_INPUT = "hdfs:///user/hadoop/i94"
HDFS_OUTPUT = "hdfs:///user/hadoop/analytics"
TEMPERATURE_FILE = "fact_temperature_state"
STATE_FILE = "dim_state"
I94_PATH = "18-83510-I94-Data-2016/"
I94_FILE = "18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat"

# Helper functions


def get_files(path):
    """ returns a list of fully qualified files for the gven path """
    # temp code to reduce total number of files we're reading
    file_list = []
    count = 0
    for item in listdir(path):
        # ! temp code to limit number of files to 1 for inital test purposes
        count = count + 1
        if count < 2:
            # ! end of temp
            if isfile(join(path, item)):
                file_list.append(join(path, item))
    logging.info("Files detected for loading: {}".format(file_list))
    return file_list


def append_datasets(*datasets):
    """ performs a union operation on the provided datasets """
    logging.info("Running union on datasets")
    return reduce(DataFrame.unionAll, datasets)


def create_spark_session():
    """ Create spark session and return """
    logging.info("Creating spark session")
    spark = (SparkSession.builder.
             enableHiveSupport().getOrCreate())
    return spark

# Processing functions


def read_and_process_airport_data(spark, filename, df_dimension_state_table):
    """ Load the airport codes join with state dimension data to get airports with state_key"""

    logging.info("Reading airport data")
    # load the airport codes so we can map them to states
    airport_schema = R([
        Fld("ident", Str()),
        Fld("type", Str()),
        Fld("name", Str()),
        Fld("elevation_ft", Int()),
        Fld("continent", Str()),
        Fld("iso_country", Str()),
        Fld("iso_region", Str()),
        Fld("municipality", Str()),
        Fld("gps_code", Str()),
        Fld("iata_code", Str()),
        Fld("local_code", Str()),
        Fld("coordinates", Str())
    ])

    df_airport = spark.read.options(Header=True, Delimter=",").csv(
        filename, airport_schema)

    # only want the airports in the US & that map to the states

    df_airport = df_airport.filter(df_airport.iso_country == "US") \
        .join(df_dimension_state_table, F.substring(df_airport.iso_region, 4, 2) == df_dimension_state_table.state_key, "inner") \
        .select(df_airport.ident, df_airport.local_code, df_dimension_state_table.state_key)

    return df_airport


def read_i94_data(spark, filename):
    logging.info("Reading i94 data:{}".format(filename))
    """ Load the i94 arrivee data from the sas data file """
    df_i94 = spark.read.format('com.github.saurfang.sas.spark').load(
        filename, inferInt=True)

    # keep just month, age, gender, airport, and visa_code
    df_i94 = df_i94.select(df_i94.i94mon.alias("month"), df_i94.i94yr.alias("year"), df_i94.i94bir.alias("age"), df_i94.gender.alias("gender"), df_i94.i94port.alias("airport_key"), df_i94.i94visa.alias("visa_key")) \
        .withColumn("month", F.col("month").cast("Integer")) \
        .withColumn("year", F.col("year").cast("Integer")) \
        .sort("month", "year", "airport_key")

    return df_i94


def read_parquet_file(spark, filename):
    logging.info("Reading parquet data:{}".format(filename))
    """ Read the named parquet file and return it as a dataframe """
    df_input = spark.read.parquet(filename)
    return df_input


def join_and_agg_i94(df_i94, df_airport):
    """ Join the i94 data to the airport codes to get the state for each port & aggregate facts by state and month """
    logging.info("Joining airport and i94 data")

    # First join the i94 raw data to the airport table to provide us with a valid state for each arrivee
    df_i94_by_state = df_i94.join(df_airport, df_i94.airport_key == df_airport.local_code, "inner") \
        .select(df_i94.month, df_i94.year, df_i94.age, df_i94.gender, df_i94.visa_key, df_airport.state_key) \
        .sort("month", "state_key")

    # Now clean up the result and remove any nulls in the fact columns we're interested in
    df_i94_cleansed = df_i94_by_state.filter((df_i94_by_state.age.isNotNull()) & (df_i94_by_state.month.isNotNull()) & (
        df_i94_by_state.gender.isNotNull()) & (df_i94_by_state.visa_key.isNotNull()) & (df_i94_by_state.year.isNotNull()))

    # Pivot the result to aggregate count values to get counts for genders by state and month
    df_i94_fact_gender = df_i94_cleansed.groupBy("state_key", "month", "year").pivot("gender").count()\
        .sort("state_key", "year", "month")

    # Do the same for visas
    df_i94_fact_visa = df_i94_cleansed.groupBy("state_key", "month", "year").pivot("visa_key").count()\
        .sort("state_key", "year", "month")

    # Agg to get the average age per month and state, rename column and round to 2.dp
    df_i94_fact_age = df_i94_cleansed.groupBy("state_key", "month", "year").avg("age") \
        .select(df_i94_cleansed.state_key, df_i94_cleansed.month, df_i94_cleansed.year, F.round(F.col("avg(age)"), 1).alias("average_age")) \
        .sort("state_key", "month")

    # Join the age dataframe with gender counts
    df_i94_fact_age_gender = df_i94_fact_age \
        .join(df_i94_fact_gender, (df_i94_fact_age.month == df_i94_fact_gender.month) & (df_i94_fact_age.state_key == df_i94_fact_gender.state_key), "inner") \
        .drop(df_i94_fact_gender.month) \
        .drop(df_i94_fact_gender.year) \
        .drop(df_i94_fact_gender.state_key)

    # and now join that to the visa type counts
    df_i94_fact_age_gender_visa = df_i94_fact_age_gender \
        .join(df_i94_fact_visa, (df_i94_fact_age_gender.month == df_i94_fact_visa.month) & (df_i94_fact_age_gender.state_key == df_i94_fact_visa.state_key), "inner") \
        .drop(df_i94_fact_visa.month) \
        .drop(df_i94_fact_visa.state_key) \
        .drop(df_i94_fact_visa.year) \
        .withColumnRenamed("1.0", "business") \
        .withColumnRenamed("2.0", "pleasure") \
        .withColumnRenamed("3.0", "student") \
        .sort(df_i94_fact_age_gender.state_key, df_i94_fact_age_gender.month, df_i94_fact_age_gender.year)

    # select and reorder the columns that we want
    df_fact_i94_age_gender_visa = df_i94_fact_age_gender_visa.select(df_i94_fact_age_gender_visa.state_key, df_i94_fact_age_gender_visa.month, df_i94_fact_age_gender_visa.year, df_i94_fact_age_gender_visa.average_age, df_i94_fact_age_gender_visa.F,
                                                                     df_i94_fact_age_gender_visa.M, df_i94_fact_age_gender_visa.U, df_i94_fact_age_gender_visa.X, df_i94_fact_age_gender_visa.business, df_i94_fact_age_gender_visa.pleasure, df_i94_fact_age_gender_visa.student)

    return df_fact_i94_age_gender_visa


def build_fact_table(df_fact_i94_age_gender_visa, df_fact_temperature_by_state_key):
    """ Final join between temperature data by state and arrivals facts by state to build fact table """
    # Now we can join this with the temperature data in the temp fact table
    # join on state address and month

    logging.info("Building the fact table")
    df_fact_arrivals_table = df_fact_i94_age_gender_visa \
        .join(df_fact_temperature_by_state_key, (df_fact_i94_age_gender_visa.month == df_fact_temperature_by_state_key.month) & (df_fact_i94_age_gender_visa.state_key == df_fact_temperature_by_state_key.state_key), "inner") \
        .drop(df_fact_temperature_by_state_key.state_key) \
        .drop(df_fact_temperature_by_state_key.month)

    df_fact_arrivals_table.show(100)

    return df_fact_arrivals_table


def write_parquet(dataset, output_file):
    """ Output provided dataset to parquet file for use later """

    logging.info("writing fact table")
    dataset.write.parquet(output_file)


def main():
    """ Main Routine """


# create spark session
spark = create_spark_session()
df_dimension_state_table = read_parquet_file(
    spark, HDFS_OUTPUT + '/' + STATE_FILE)
# Read the airport codes datafile csv
df_airport = read_and_process_airport_data(
    spark, HDFS_INPUT + '/' + INPUT_FILE, df_dimension_state_table)
# get a list of all the datafiles in the i94 directory
i94_datafile_list = get_files(I94_PATH)
# iterate over the list to create a list of data sets
i94_total_data_set = map(
    lambda filename: read_i94_data(spark, filename), i94_datafile_list)
# union them all toegther so we can work with them
df_i94 = append_datasets(*i94_total_data_set)

# Join the i94 data to the airport codes on local_code to give each arrivee a state_key
# summarise i94 data by gender count, avergae age and count of visa type
df_fact_i94_age_gender_visa = join_and_agg_i94(df_i94, df_airport)
# Now get the temperature data we've already processed by month and state
df_fact_temperature_by_state_key = read_parquet_file(
    spark, HDFS_OUTPUT + '/' + TEMPERATURE_FILE)
# Do the final join to add the temperature data to the arrival fact table
df_fact_arrivals_table = build_fact_table(
    df_fact_i94_age_gender_visa, df_fact_temperature_by_state_key)
df_fact_arrivals_table.show()
# Store the final fact table
write_parquet(df_fact_arrivals_table,
              HDFS_OUTPUT + '/' + OUTPUT_FILE)
