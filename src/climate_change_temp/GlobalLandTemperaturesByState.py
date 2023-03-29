import pyspark as spark

def maxtemp(input_csv, output_parquet):
    """
    Find max temp for country, state based on csv file input_csv and save date to output_parquet file in parquet format
    :param input_csv:
    :param output_parquet:
    :return: datarame with save date to output_parquet file in parquet format
    """
    df=readDf(input_csv)
    df_grouped=maxtemp_max_grouby(df)
    writeDf(df_grouped,output_parquet)

def maxtemp_max_grouby(df):
    """
    Return DataFrame with maxroup by Country,State
    :param df: DataFrame with sourced required columns: Country, State, AverageTemperature
    :return: datarame with max temp group by Country,State
    """
    df2=df.groupBy("Country","State")\
        .max("AverageTemperature")\
        .withColumnRenamed("max(AverageTemperature)", "maxAvrgTemp")
    return df2

def readDf(filename):
    """
    Read csv from file to SparkDataframe
    # File location and type for example "/FileStore/tables/GlobalLandTemperaturesByState/GlobalLandTemperaturesByState.csv"
    dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState.csv

    :param filename: input CSV filename for GlobalLandTemperaturesByState
    :return: SparkDataframe with read file

    """
    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("sep", ",") \
        .load(filename)
    return df

def writeDf(df, permanent_parquet_dir):
    """
    Write SparkDataframe to output file in parquet format
    permanent_parquet_dir = "dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState_parquet"
    :param df: dataframe
    :param permanent_parquet_dir: : parquet dir filename with path to save result
    :return: save date to output_parquet file in parquet format
    """
    df.write.parquet(permanent_parquet_dir, mode='overwrite')

