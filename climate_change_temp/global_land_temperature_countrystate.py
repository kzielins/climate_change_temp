from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging


class GlobalLandTemperaturesByCountryState:

    global_land_temperatyre_csv_schema = StructType([
        StructField("dt",                   StringType(), True),
        StructField("AverageTemperature",   DoubleType(),   True),
        StructField("AverageTemperatureUncertainty",     DoubleType(),  True),
        StructField("State",                 StringType(),  True),
        StructField("Country",               StringType(),  True),
        ])

    _spark = None

    def __init__(self, spark):
        self._spark = spark


    def calculate_max_temperature(self, input_csv, output_parquet):
        """
        Find/calculate max temperature for country, state based on csv file input_csv and save date to
            output_parquet file in parquet format

        :param input_csv: input csv file path with filename
        :param output_parquet: dest dir to write transformed data in parquet format
        :return: dataframe with save date to output_parquet file in parquet format
        """

        # TODO check input_csv with DBUtils
        # with def get_dbutils(spark):

        # 1. read temperatures from csv to df
        raw_temperatures_df = self.read_dataframe_from_csv(input_csv)
        # 2. calculate max temperature for country,state
        max_temp_df = self.calculate_max_temperature_dataframe(raw_temperatures_df)
        # 3 save dataframe to parquet file
        self.write_dataframe_to_parquet(max_temp_df, output_parquet)

    def calculate_max_temperature_dataframe(self, raw_temperatures_df):
        """
        Return DataFrame with max temperature grouped by Country,State
        :param raw_temperatures_df: DataFrame with sourced required columns: Country, State, AverageTemperature
        :return: dataframe with max temp group by Country,State
        """

        if raw_temperatures_df is None:
            print("initiate/read df, eg: using read_dataframe_from_csv")
            raise Exception("initiate/read df, eg: using read_dataframe_from_csv")
        # TODO check csv structure and return correct error
        max_temperatures_df = raw_temperatures_df.groupBy("Country", "State")\
            .max("AverageTemperature")\
            .withColumnRenamed("max(AverageTemperature)", "maxAvrgTemp")
        return max_temperatures_df

    def read_dataframe_from_csv(self, csv_filename, inferschema=True):
        """
        Read csv to SparkDataframe
        # File location and type for example "/FileStore/tables/GlobalLandTemperaturesByState/
          GlobalLandTemperaturesByState.csv"
        dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState.csv

        :param csv_filename: input CSV filename for GlobalLandTemperaturesByState
        :param inferschema: for True schema detected based on csv file , for False csv schema taken from csvSchema
        :return: SparkDataframe with read file
        """
        if self._spark is None:
            logging.error('initiate Sparc variable')
            raise Exception("initiate sparc required with set_spark(Spark)")

        # TODO check csv file exists and return correct error
        # with def get_dbutils(spark):

        # TODO check csv structure and return correct error
        try:
            if inferschema is True:
                raw_temperatures_df = self._spark.read.format("csv") \
                    .option("inferSchema", "true") \
                    .option("header", "true") \
                    .option("sep", ",") \
                    .load(csv_filename)
            else:
                raw_temperatures_df = self._spark.read.format("csv") \
                    .option("header", "true") \
                    .option("sep", ",") \
                    .schema(self.global_land_temperatyre_csv_schema)\
                    .load(csv_filename)
        except:
            logging.error("Failed to read csv file:"+csv_filename)
            raise Exception("Failed to read csv file:"+csv_filename)
        finally:
            return raw_temperatures_df

    def write_dataframe_to_parquet(self, max_temperatures_df, permanent_parquet_dir):
        """
        Write SparkDataframe to output file in parquet format
        permanent_parquet_dir = "dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/
           GlobalLandTemperaturesByState_parquet"
        :param max_temperatures_df: dataframe with results
        :param permanent_parquet_dir: : parquet dir filename with path to save result
        :return: save date to output_parquet file in parquet format
        """

        if max_temperatures_df is None:
            logging.error("initiate/read df, eg: using read_dataframe_from_csv")
            raise Exception("initiate/read df, eg: using read_dataframe_from_csv")

        # TODO check if destination dir exists with dbutils.fs.ls(permanent_parquet_dir)
        # with def get_dbutils(spark):

        try:
            max_temperatures_df.write.parquet(permanent_parquet_dir, mode='overwrite')
        except:
            logging.error("Failed to write Dataframe to "+permanent_parquet_dir)
            raise Exception("Failed to write Dataframe to "+permanent_parquet_dir)
        finally:
            logging.info("Dataframe with results written successfully to "+permanent_parquet_dir)
