import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.climate_change_temp.GlobalLandTemperaturesByStateObj import *

import pandas as pd
import numpy as np


class GlobalLandTemperaturesByStateTests(unittest.TestCase):

    data = [
            ['Poland','Warsaw', 15,1998],
            ['Poland','Warsaw', 16.7,2020],
            ['Germany','Bonn', 18,2001],
            ['Germany','Bonn', 17.2,2010],
            ['Germany','Bonn', 19.9,2020],
            ['Australia','Sydney', 30,2000],
            ['Australia','Sydney', 31,2010],
            ['Australia','Sydney', 30.2,2020],
            ['Australia','Sydney', 35,2022],
            ['US','NY',30.1,2004],
            ['US','NY',31,2010],
            ['US','NY',32.2,2020],
            ['US','NY',33,2022]]

    pDF = pd.DataFrame(data, columns = ['Country', 'State','AverageTemperature','Year'])

    NYdata= [['US','NY',33.0]]
    pdNY=pd.DataFrame(NYdata,columns= ['Country','State','maxAvrgTemp'])


    def test_df_Poland_1(self):
        self.assertEqual(self.pDF['Country'][1], 'Poland', "Should be Poland")
    def test_df_US_11(self):
        self.assertEqual(self.pDF['Country'][11], 'US', "Should be US")
    def test_maxTempContryState(self):
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("KZGlobalLandTempTest") \
            .getOrCreate()

        sparkDF=spark.createDataFrame(self.pDF)
        glT = GlobalLandTemperaturesByStateObj(spark)
        max_df=glT.maxTempContryState(sparkDF)
        #max_df.show()
        fdf=max_df.filter("State == 'NY'")
        pdf = fdf.toPandas()
        pd.testing.assert_frame_equal(pdf,self.pdNY)

    #TODO test write parquet
    #glT.writeToParquet('parquet')

if __name__ == '__main__':
    GlobalLandTemperaturesByStateTests.main()
