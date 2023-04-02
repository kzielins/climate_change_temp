import unittest
from pyspark.sql import SparkSession
from climate_change_temp.global_land_temperature_countrystate import GlobalLandTemperaturesByCountryState

import pandas as pd


class GlobalLandTemperaturesByCountryStateTests(unittest.TestCase):
    #test data
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

    #expected results
    NYdata= [['US','NY',33.0]]
    pdNY=pd.DataFrame(NYdata,columns= ['Country','State','maxAvrgTemp'])

    # test read Poland
    def test_df_Poland_1(self):
        self.assertEqual(self.pDF['Country'][1], 'Poland', "Should be Poland")

    # test read US
    def test_df_US_11(self):
        self.assertEqual(self.pDF['Country'][11], 'US', "Should be US")

    # test max calculate by cuntry and state
    def test_maxTempContryState(self):
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("GlobalLandTemperaturesTest") \
            .getOrCreate()

        sparkDF=spark.createDataFrame(self.pDF)
        glT = GlobalLandTemperaturesByCountryState(spark)
        glT.set_spark(spark)
        max_df=glT.calculate_max_temperature_dataframe(sparkDF)
        # max_df.show()
        fdf=max_df.filter("State == 'NY'")
        pdf = fdf.toPandas()
        pd.testing.assert_frame_equal(pdf,self.pdNY)
        # close sparkContext
        spark.sparkContext.stop()

    # TODO test write parquet
    # glT.write_dataframe_to_parquet('parquet')

if __name__ == '__main__':
    GlobalLandTemperaturesByCountryState.main()
