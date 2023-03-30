import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DoubleType

#TODO import pyspark dbutils for testing parquet destination dir
#from pyspark.dbutils import DBUtils


class GlobalLandTemperaturesByStateObj:


    csvSchema = StructType([ \
        StructField("dt",                   StringType(), True), \
        StructField("AverageTemperature",   DoubleType(),   True), \
        StructField("AverageTemperatureUncertainty",     DoubleType(),  True), \
        StructField("State",                 StringType(),  True), \
        StructField("Country",               StringType(),  True), \
        ])

    sparc = None
    def __init__(self, sparc_ref ):
        #print(self.sparc)
        self.sparc = sparc_ref
        #print(self.sparc)

    def setSpark(self, sparc_ref ):
        self.sparc = sparc_ref


    def calculate_max_temperature(self, input_csv, output_parquet):
        """
        Find/calculate max temp for country, state based on csv file input_csv and save date to output_parquet file in parquet format
        :param spark_ spark object
        :param input_csv: imput csv file path with filename
        :param output_parquet: dest dir to write transformed data in pqrquet format
        :return: datarame with save date to output_parquet file in parquet format
        """
        #TODO error checks
        df=self.readFromCsv(input_csv)

        df_grouped=self.maxTempContryState(df)
        self.writeToParquet(df_grouped,output_parquet)

    def maxTempContryState(self,df):
        """
        Return DataFrame max temp group by Country,State
        :param df: DataFrame with sourced required columns: Country, State, AverageTemperature
        :return: datarame with max temp group by Country,State
        """

        if df is None:
            print("initiate/read df, eg: using readFromCsv")
            raise Exception("initiate/read df, eg: using readFromCsv")
        #TODO check csv structure and return correct error
        df2=df.groupBy("Country","State")\
            .max("AverageTemperature")\
            .withColumnRenamed("max(AverageTemperature)", "maxAvrgTemp")
        return df2

    def readFromCsv(self,csv_filename,inferschema = True):
        """
        Read csv from file to SparkDataframe
        # File location and type for example "/FileStore/tables/GlobalLandTemperaturesByState/GlobalLandTemperaturesByState.csv"
        dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState.csv

        :param csv_filename: input CSV filename for GlobalLandTemperaturesByState
        :return: SparkDataframe with read file

        """
        if self.sparc is None:
            print("initiate sparc in constructor")
            raise Exception("initiate sparc in constructor")

        #TODO check csv file exists and return correct error
        #TODO check csv structure and return correct error
        try:
            if (inferschema == True):
                df = self.spark.read.format("csv") \
                    .option("inferSchema", "true") \
                    .option("header", "true") \
                    .option("sep", ",") \
                    .load(csv_filename)
            else:
                df = self.spark.read.format("csv") \
                    .option("header", "true") \
                    .option("sep", ",") \
                    .schema(self.csvSchema)\
                    .load(csv_filename)
        except:
            print("Failed to read csv file:"+csv_filename)
            raise Exception("Failed to read csv file:"+csv_filename)
        finally:
            return df

    def writeToParquet(self,df, permanent_parquet_dir):
        """
        Write SparkDataframe to output file in parquet format
        permanent_parquet_dir = "dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState_parquet"
        :param df: dataframe with results
        :param permanent_parquet_dir: : parquet dir filename with path to save result
        :return: save date to output_parquet file in parquet format
        """

        if df is None:
            print("initiate/read df, eg: using readFromCsv")
            raise Exception("initiate/read df, eg: using readFromCsv")

        #TODO check if destination dir exists
        try:
            print("write test to check if destination dir exists ")
            #dbutils.fs.ls(permanent_parquet_dir)
        except:
            print("Dest dir does not exist: " + permanent_parquet_dir)

        #TODO check destination dir exists and return correct error
        try:
            df.write.parquet(permanent_parquet_dir, mode='overwrite')
        except:
            print("Failed to write Dataframe to "+permanent_parquet_dir)
            raise Exception("Failed to write Dataframe to "+permanent_parquet_dir)
        finally:
            print("Dataframe with results written successfully to "+permanent_parquet_dir)
