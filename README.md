# Task
Find the maximum temperature for a given country and state, then write the result to a file on DBFS in parquet format.
Assumptions:
- Source data: https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download (file: GlobalLandTemperaturesByState.csv)
- Solution: source code on github (with unit tests), package published in PyPI (package build and upload can be manual)
- Tools: PySpark + Databricks (Community Edition) - package installed with PyPI, file to run package can be in notebook
- Job parameters: input path and output path

# Package documentation
- Python lib name "climate_change_temp" should be installed to Databrick Compute "(7.3 LTS (includes Apache Spark 3.0.1, Scala 2.12))" nodes
- CSV https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download (file: GlobalLandTemperaturesByState.csv) data should be uploaded into Databrick.
  - dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState.csv
- update databrick notebook variables  : source_csv_filename , permanent_parquet_dir

## Source code
Directory: climate_change_temp/climatechangetemperature
global_land_temperature_countrystate.py 

## Databricks scripts
Requirements :  PySpark + Databricks (Community Edition) Spark 3.0.1

Example databrick notebooks availble at: climate_change_temp/test/databrick_scripts/
File: V_climate_change_with_lib_obj_short.dbc -  databrick with object lib version

Example ussage :
``` Databrick_notebook V_climate_change_with_lib_short
#import custom package
from climatechangetemperature import global_land_temperature_countrystate
#source csv  https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download (file: GlobalLandTemperaturesByState.csv) uploaded to example dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/ 
source_csv_filename="dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState.csv"
#destination parquet dir, should be updated
permanent_parquet_dir = "dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState_parquet"
#csv to parquet transformation execution 
glt = global_land_temperature_countrystate.GlobalLandTemperaturesByCountryState(spark)
glt.calculate_max_temperature(source_csv_filename,permanent_parquet_dir)
```

## Tests
Simple Unittest climate_change_temp/tests/GlobalLandTemperaturesByStateTests.py with 2 simple tests
Non implemented potential addtional tests:
- src csv file
- src csv structure validation
- databricks directories validation


## Package build
- Package name: climatechangetemperature
- Directory: climate_change_temp/dist
- File: climate_change_temp-0.1.5-py3-none-any.whl - py whl package
- 
- Package build commands (execuded in dir climatechangetemperature/)
```
py -m build
```
