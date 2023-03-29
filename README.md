# Task
Find the maximum temperature for a given country and state, then write the result to a file on DBFS in parquet format.
Assumptions:
- Source data: https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download (file: GlobalLandTemperaturesByState.csv)
- Solution: source code on github (with unit tests), package published in PyPI (package build and upload can be manual)
- Tools: PySpark + Databricks (Community Edition) - package installed with PyPI, file to run package can be in notebook
- Job parameters: input path and output path


# Usefull links  
## Manage python packages Azure Databricks
https://learn.microsoft.com/en-us/azure/databricks/libraries/notebooks-python-libraries

### Install a library with %pip
''' Python
%pip install matplotlib
'''

### Install a library from a version control system with %pip
''' Python
%pip install git+https://github.com/databricks/databricks-cli
'''


### Save libraries in a requirements file
''' Python
%pip freeze > /dbfs/requirements.txt
'''

### Use a requirements file to install libraries
''' Python
%pip install -r /dbfs/requirements.txt
'''
