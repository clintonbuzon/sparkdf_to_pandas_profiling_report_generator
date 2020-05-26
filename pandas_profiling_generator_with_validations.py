import sys
from datetime import datetime
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pandas_profiling import ProfileReport
from bs4 import BeautifulSoup

# Generate pandas_profiling format html table tag
# Inputs:
# string_table_html: raw string formatted table html
# title_table_html: Title of table to be used on html report
def generateTableTag(string_table_html, title_table_html):
    soup = BeautifulSoup(string_table_html)
    soup.find('table')['class'] = "dataframe duplicate table table-striped"
    
    # Create tags
    section_items_tag = soup.new_tag("div", **{'class': 'section-items'})
    row_spacing_tag = soup.new_tag("div", **{'class': 'row spacing'})
    h2_tag = soup.new_tag("h2", **{'class':'indent'})
    h2_tag.append(title_table_html)
    
    # Construct new div
    container_tag = soup.new_tag("div", id="sample-container" ,**{'class':'col-sm-12'})
    container_tag.append(soup.find('table'))
    row_spacing_tag.append(h2_tag)
    row_spacing_tag.append(container_tag)
    section_items_tag.append(row_spacing_tag)
    
    # Return new div with table title and table data
    return section_items_tag

# Append custom validation parts of an existing pandas profile html report
# Inputs:
# pandas_profiling_report: full path of pandas profiling report where we would append custom validation data
# spark_dataset_list: list of spark datasets to be used when appending custom validation data to existing report
def generateCustomValidations(pandas_profiling_report, spark_dataset_list):
    soup = BeautifulSoup(open(pandas_profiling_report, 'r'))

    navbar = soup.find_all('ul', **{'class': 'nav navbar-nav navbar-right'})[-1]

    li_tag = soup.new_tag('li')
    a_tag = soup.new_tag('a', href="#validations", **{'class': 'anchor'})
    a_tag.append("Validations")
    li_tag.append(a_tag)
    navbar.append(li_tag)

    # Create tags
    temp = soup.find_all('div', **{'class': 'section-items'})[-1]
    row_header_tag = soup.new_tag("div", **{'class': 'row header'})
    a_tag = soup.new_tag('a', id="validations", **{'class': 'anchor-pos'})
    h1_tag = soup.new_tag("h1", **{'class':'page-header'})
    h1_tag.append("Custom Validations")
    row_header_tag.append(a_tag)
    row_header_tag.append(h1_tag)
    
    # Go through each spark dataframe
    spark_dataset_list.reverse() # added reverse since we want first dataset to appear at top
    for spark_dataset in spark_dataset_list:
        table_tag = generateTableTag(spark_dataset['data'].toPandas().to_html(),spark_dataset['title'])
        temp.insert_after(table_tag)
    temp.insert_after(row_header_tag)
    
    # Save file
    updated_file = pandas_profiling_report.replace(".html","_validations.html")
    with open(updated_file, "w") as outf:
        outf.write(str(soup))

########## Main ##########

# Check if enough parameters were passed during execution
if len(sys.argv) < 3:
    print("No parametres specified") 
    print("Parameter 1: source file name") 
    print("Parameter 2: report name to be generated")
    print("Example: {} '/Users/clintonbuzon/Downloads/sra_postpaid/unit/pxn_sync_dt=2020-01' SRA".format(sys.argv[0]))
    exit(0)
	
# Set input parameters as variables
source_data = sys.argv[1]
report_dir = "sample_output"
report_name = "{}_{}.{}".format(sys.argv[2],datetime.today().strftime('%Y%m%d%H%M%S'),'html')
report_full_path = "{}/{}".format(report_dir,report_name)

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
prof.to_file(output_file=report_full_path)

# Custom validations part here
if report_name[:-20] == 'voicesms_forecast':
    # Spark Datasets
    totalSmsCount = df.groupBy("segment").agg(f.sum('totalSmsCount').cast("Decimal(30,2)").alias("sum(totalSmsCount)"))
    totalCallDuration = df.groupBy("segment").agg(f.sum('totalCallDuration').cast("Decimal(30,2)").alias("sum(totalCallDuration)"))
    
    # Compile allspark datasets to spark_dataset_list
    spark_dataset_list = []
    spark_dataset1 = {'data':totalSmsCount,'title':'Total SMS per segment'}
    spark_dataset2 = {'data':totalCallDuration,'title':'Total Voice per segment'}
    spark_dataset_list.append(spark_dataset1)
    spark_dataset_list.append(spark_dataset2)
    
    # Pass spark_dataset_list to report generation function
    generateCustomValidations(report_full_path, spark_dataset_list)

if report_name[:-20] == 'throughput_index_consumer_cell':
    cxCellAgg=df.groupBy("unitId","ratType").agg(f.min("avgThroughput").alias("minThrp"),f.max("avgThroughput").alias("maxThrp")).limit(10)

    spark_dataset_list = []
    spark_dataset1 = {'data':cxCellAgg,'title':'Check that the site one isnt higher than the max of the cells, or lower than the min of the cell'}
    spark_dataset_list.append(spark_dataset1)
    
    generateCustomValidations(report_full_path, spark_dataset_list)


