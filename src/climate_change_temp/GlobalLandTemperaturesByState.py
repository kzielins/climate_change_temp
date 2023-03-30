import pyspark

#TODO consider object refactoring
def maxtemp(spark_,input_csv, output_parquet):
    """
    Find max temp for country, state based on csv file input_csv and save date to output_parquet file in parquet format
    :param spark_ spark object
    :param input_csv: imput csv file path with filename
    :param output_parquet:
    :return: datarame with save date to output_parquet file in parquet format
    """
    #TODO error check
    df=readDf(spark_,input_csv)
    df_grouped=maxtemp_max_grouby(df)
    writeDf(df_grouped,output_parquet)

def maxtemp_max_grouby(df):
    """
    Return DataFrame with maxroup by Country,State
    :param df: DataFrame with sourced required columns: Country, State, AverageTemperature
    :return: datarame with max temp group by Country,State
    """
    #TODO check csv structure and return correct error
    df2=df.groupBy("Country","State")\
        .max("AverageTemperature")\
        .withColumnRenamed("max(AverageTemperature)", "maxAvrgTemp")
    return df2

def readDf(spark_,filename):
    """
    Read csv from file to SparkDataframe
    # File location and type for example "/FileStore/tables/GlobalLandTemperaturesByState/GlobalLandTemperaturesByState.csv"
    dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState.csv

    :param spark_ spark object
    :param filename: input CSV filename for GlobalLandTemperaturesByState
    :return: SparkDataframe with read file

    """
    #TODO check csv file exists and return correct error
    #TODO check csv structure and return correct error
    try:
        df = spark_.read.format("csv") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(filename)
    except:
        print("Failed to read csv file:"+filename)
        raise Exception("Failed to read csv file:"+filename)
    finally:
        return df

def writeDf(df, permanent_parquet_dir):
    """
    Write SparkDataframe to output file in parquet format
    permanent_parquet_dir = "dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState_parquet"
    :param df: dataframe with results
    :param permanent_parquet_dir: : parquet dir filename with path to save result
    :return: save date to output_parquet file in parquet format
    """
    #TODO check destination dir exists and return correct error
    try:
        df.write.parquet(permanent_parquet_dir, mode='overwrite')
    except:
        print("Failed to write Dataframe to "+permanent_parquet_dir)
    finally:
        print("Dataframe with results written successfully to "+permanent_parquet_dir)
