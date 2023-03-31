# Task
Find the maximum temperature for a given country and state, then write the result to a file on DBFS in parquet format.
Assumptions:
- Source data: https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download (file: GlobalLandTemperaturesByState.csv)
- Solution: source code on github (with unit tests), package published in PyPI (package build and upload can be manual)
- Tools: PySpark + Databricks (Community Edition) - package installed with PyPI, file to run package can be in notebook
- Job parameters: input path and output path

# Solution documentation
Python lib name "climate_change_temp"

## Source code
  Directory: src
1. GlobalLandTemperaturesByState.py - 1st  non object version
2. GlobalLandTemperaturesByStateObj.py - 2nd obj version

## Databricks scripts 
Requirements :  PySpark + Databricks (Community Edition) Spark 3.0.1
  /test/databrick_scripts/
1. File: V_climate_change_with_lib_short.dbc - databrick with non object lib version
2. File: V_climate_change_with_lib_obj_short.dbc -  databrick with object lib version

Example ussage :
  from climate_change_temp.GlobalLandTemperaturesByState import *
  source_csv_filename="dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState.csv"
  permanent_parquet_dir = "dbfs:/FileStore/shared_uploads/krzychzet@gmail.com/GlobalLandTemperaturesByState_parquet"
  maxtemp(spark,source_csv_filename,permanent_parquet_dir)

## Install lib 
  Directory: dist
1. climate_change_temp-0.1.0-py3-none-any.whl - py whl package



# Usefull links  
## Python library development and build
   py -m build
