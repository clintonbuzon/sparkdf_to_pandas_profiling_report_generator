# Generate Pandas Profiling Report using Spark Dataframe

Simple pyspark application which takes spark dataframe as input and automatically converts it to pandas dataframe, then generate pandas profiling report in html format.

Pandas profiling is usually being used for Exploratory Data Analysis (EDA). This provides us quick insights to our dataset, which we sometimes miss, or did not think about.

## Prerequisites

### Applications

 - Java8
 - scala
 - apache-spark
 - python3
	 - `pip3 install pyspark`
	 - `pip3 install pandas`
	 - `pip3 install pandas_profiling`
	 - `pip3 install bs4`

### Python modules 

#### pyspark

[**PySpark**](https://spark.apache.org/docs/latest/api/python/index.html) is the Python API written in python to support Apache Spark. Apache Spark is a distributed framework that can handle Big Data analysis. We use this mostly to read Spark Dataframes and convert them easily to Pandas Dataframes using `toPandas()` function. Converting data frames to pandas gives us the ability to utilize prexisting python libraries such as `pandas_profiling`

#### pandas

[**pandas**](https://github.com/pandas-dev/pandas) is a Python package providing fast, flexible, and expressive data structures designed to make working with "relational" or "labeled" data both easy and intuitive. It aims to be the fundamental high-level building block for doing practical, **real world** data analysis in Python. Additionally, it has the broader goal of becoming **the most powerful and flexible open source data analysis / manipulation tool available in any language**. It is already well on its way towards this goal.

#### pandas_profiling

[**pandas_profiling**](https://github.com/pandas-profiling/pandas-profiling) generates profile reports from a pandas `DataFrame`. The pandas `df.describe()` function is great but a little basic for serious exploratory data analysis. `pandas_profiling` extends the pandas DataFrame with `df.profile_report()` for quick data analysis. This also allows us to create an output html report using `to_file(output_file=output.html)`

#### bs4 (also known as Beautiful soup)

[**Beautiful Soup**](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)  is a Python library for pulling data out of HTML and XML files. It works with your favorite parser to provide idiomatic ways of navigating, searching, and modifying the parse tree. It is commonly used for html parsing, manipulation and web scraping. We will use this to append new data on default pandas profiling html report.

## Usage

We have 2 types of pyspark scripts.
- `pandas_profiling_generator.py`
- `pandas_profiling_generator_with_validations.py`

Both accepts 2 parameters,
1. source data - full path of directory containing parquet files
2. report name - name or title of report, which we can also use to add custom validations

Scripts are passed to `spark-submit` when being executed.

### Example

#### regular pandas profiling report
```bash
spark-submit pandas_profiling_generator.py /Users/clintonbuzon/Downloads/source_data/latentDemand_fourG latentDemand_fourG
```

#### report with custom validations

```bash
spark-submit pandas_profiling_generator_with_validations.py /Users/clintonbuzon/Downloads/voicesms_forecast_0_201911 voicesms_forecast
```

## How to add custom validations

### Add new code block on the bottom of pandas_profiling_generator_with_validations.py

Example:

```python
if report_name[:-20] == 'voicesms_forecast':
    totalSmsCount = df.groupBy("segment").agg(f.sum('totalSmsCount').cast("Decimal(30,2)").alias("sum(totalSmsCount)"))
    totalCallDuration = df.groupBy("segment").agg(f.sum('totalCallDuration').cast("Decimal(30,2)").alias("sum(totalCallDuration)"))
    
    spark_dataset_list = []
    spark_dataset1 = {'data':totalSmsCount,'title':'Total SMS per segment'}
    spark_dataset2 = {'data':totalCallDuration,'title':'Total Voice per segment'}
    spark_dataset_list.append(spark_dataset1)
    spark_dataset_list.append(spark_dataset2)
    
    generateCustomValidations(report_full_path, spark_dataset_list)
```
