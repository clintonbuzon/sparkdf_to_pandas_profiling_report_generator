import sys
from datetime import datetime
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pandas_profiling import ProfileReport

# Check if enough parameters were passed during execution
if len(sys.argv) < 3:
    print("No parametres specified") 
    print("Parameter 1: source file name") 
    print("Parameter 2: report name to be generated")
    print("Example: {} '/Users/clintonbuzon/Downloads/sra_postpaid/unit/pxn_sync_dt=2020-01' SRA".format(sys.argv[0]))
    exit(0)
	
# Set input parameters as variables
source_data = sys.argv[1]
report_name = "{}_{}.{}".format(sys.argv[2],datetime.today().strftime('%Y%m%d%H%M%S'))

# Create spark session
spark = SparkSession.builder \
    .master('local') \
    .appName('myAppName') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

# To read parquet file as spark dataframe
df = sqlContext.read.parquet(source_data)
print("Spark Dataframe")
df.printSchema()

# Convert spark dataframe to pandas dataframe
print("Pandas Dataframe")
pandas_df = df.toPandas()

# Replace all blank balues as np.nan, this provides pandas_profiling better picture on the data when checking for missing values
pandas_df.replace("", np.nan, inplace=True)
pandas_df.info()

# Generate pandas profiling report
prof = ProfileReport(pandas_df)
prof.to_file(output_file=report_name)
