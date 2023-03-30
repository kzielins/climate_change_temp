# Task
Find the maximum temperature for a given country and state, then write the result to a file on DBFS in parquet format.
Assumptions:
- Source data: https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download (file: GlobalLandTemperaturesByState.csv)
- Solution: source code on github (with unit tests), package published in PyPI (package build and upload can be manual)
- Tools: PySpark + Databricks (Community Edition) - package installed with PyPI, file to run package can be in notebook
- Job parameters: input path and output path

#Solution docs
 /test/databrick_scripts/
 V_climate_change_with_lib_short.dbc - non object lib version
 V_climate_change_with_lib_obj_short.dbc -  object lib version

# Usefull links  
## Python library development and build
https://packaging.python.org/en/latest/tutorials/packaging-projects/
py -m build
