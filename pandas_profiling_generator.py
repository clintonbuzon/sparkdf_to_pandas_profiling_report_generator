import sys
from datetime import datetime
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pandas_profiling import ProfileReport

if len(sys.argv) < 3:
    print("No parametres specified") 
    print("Parameter 1: source file name") 
    print("Parameter 2: report name to be generated")
    print("Example: {} '/Users/clintonbuzon/Downloads/sra_postpaid/unit/pxn_sync_dt=2020-01' SRA".format(sys.argv[0]))
    exit(0)
	
source_data = sys.argv[1]
report_name = "{}_{}.{}".format(sys.argv[2],datetime.today().strftime('%Y%m%d%H%M%S'))
source_data = '/Users/clintonbuzon/Downloads/voicesms_forecast_20200428/voicesms_forecast_0_201911'
report_name = "{}_{}.{}".format('voicesms_forecast',datetime.today().strftime('%Y%m%d%H%M%S'),'html')

spark = SparkSession.builder \
    .master('local') \
    .appName('myAppName') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

# to read parquet file as spark dataframe
df = sqlContext.read.parquet(source_data)
print("Spark Dataframe")
df.printSchema()

# convert spark dataframe to pandas dataframe
print("Pandas Dataframe")
pandas_df = df.toPandas()
pandas_df.info()

# generate pandas profiling report
prof = ProfileReport(pandas_df)
prof.to_file(output_file=report_name)
