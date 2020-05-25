import sys
from datetime import datetime
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pandas_profiling import ProfileReport
from bs4 import BeautifulSoup

def generateTableTag(string_table_html, title_table_html):
    soup = BeautifulSoup(string_table_html)
    soup.find('table')['class'] = "dataframe duplicate table table-striped"
    section_items_tag = soup.new_tag("div", **{'class': 'section-items'})
    row_spacing_tag = soup.new_tag("div", **{'class': 'row spacing'})
    h2_tag = soup.new_tag("h2", **{'class':'indent'})
    h2_tag.append(title_table_html)
    container_tag = soup.new_tag("div", id="sample-container" ,**{'class':'col-sm-12'})
    container_tag.append(soup.find('table'))
    row_spacing_tag.append(h2_tag)
    row_spacing_tag.append(container_tag)
    section_items_tag.append(row_spacing_tag)
    return section_items_tag

def generateCustomValidations(pandas_profiling_report, spark_dataset_list):
    soup = BeautifulSoup(open(pandas_profiling_report, 'r'))
    temp = soup.find_all('div',{'class': 'section-items'})[-1]
    row_header_tag = soup.new_tag("div", **{'class': 'row header'})
    h1_tag = soup.new_tag("h1", **{'class':'page-header'})
    h1_tag.append("Custom Validations")
    row_header_tag.append(h1_tag)
    spark_dataset_list.reverse()
    for spark_dataset in spark_dataset_list:
        table_tag = generateTableTag(spark_dataset['data'].toPandas().to_html(),spark_dataset['title'])
        temp.insert_after(table_tag)
    temp.insert_after(row_header_tag)
    # save the file again
    with open("temp.html", "w") as outf:
        outf.write(str(soup))


# main
if len(sys.argv) < 3:
    print("No parametres specified") 
    print("Parameter 1: source file name") 
    print("Parameter 2: report name to be generated")
    print("Example: {} '/Users/clintonbuzon/Downloads/sra_postpaid/unit/pxn_sync_dt=2020-01' SRA".format(sys.argv[0]))
    exit(0)
	
source_data = sys.argv[1]
report_name = "{}_{}.{}".format(sys.argv[2],datetime.today().strftime('%Y%m%d%H%M%S'),'html')

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
pandas_df.replace("", np.nan, inplace=True)
pandas_df.info()

# generate pandas profiling report
prof = ProfileReport(pandas_df)
prof.to_file(output_file=report_name)

#custom validations here
if report_name[:-20] == 'voicesms_forecast':
    totalSmsCount = df.groupBy("segment").agg(f.sum('totalSmsCount').cast("Decimal(30,2)"))
    totalCallDuration = df.groupBy("segment").agg(f.sum('totalSmsCount').cast("Decimal(30,2)"))
    spark_dataset_list = []
    spark_dataset1 = {'data':totalSmsCount,'title':'Total SMS per segment'}
    spark_dataset2 = {'data':totalCallDuration,'title':'Total Voice per segment'}
    spark_dataset_list.append(spark_dataset1)
    spark_dataset_list.append(spark_dataset2)
    generateCustomValidations(report_name, spark_dataset_list)


