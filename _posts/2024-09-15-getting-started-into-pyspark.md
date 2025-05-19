---
title: "Getting Started into PySpark"
date: 2024-09-15 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python
---

# Getting Started Into PySpark

     PySpark is the Python API for Apache Spark

We covered the Intoduction to PySpark in the previous blog. Let's start diving into code.

### Install PySpark

```python 
pip install pyspark
```

Note: 
- If you are using Databricks, PySpark should be configured automatically. You can start using this by simply writing spark.
Eg: spark.sql()
- If you are using, Jupyter, you may need to ensure the environment is set up.
- Now we will see how we can utilize PySpark in local IDE.

### Create SparkSession

- Every PySpark program starts with creating a SparkSession.
- It is the entry point to Spark functionality.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GettingStartedToPySpark") \
    .getOrCreate()
```
### Using PySpark with Delta

```python
def create_spark_session_with_delta():
    warehouse_location = abspath("spark-warehouse")
        derby_home = abspath("")
        print(f"warehouse_location: {warehouse_location}")
        print("derby_home: %s", derby_home)
        derby_lock_files = glob("metastore_db/*.lck")
        for file in derby_lock_files:
            print(f"Removing derby lock file: {file}")
            os.remove(file)
        print("Removed derby lock files")
        return configure_spark_with_delta_pip(
            SparkSession.builder.appName("GettingStartedToPySparkWithDelta")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:conf/log4j.properties")
        ).getOrCreate()
```

With this code snippet you can use Delta feature on your local IDE, which enables you to create different schemas and tables with views.

### Load Sample Data

```python
df = spark.read.csv("path_to_csv_file.csv", header=True, inferSchema=True)
```
With this you can create Dataframe based out of file. Dataframe is equivalent to tables in relational DB.

PySpark supports various file types such as:
- spark.read.csv('path')
- spark.read.parquet('path')
- spark.read.json('path')
- spark.read.spark.read.format("avro").load('path') -> You need to install 'spark-avro' package
- spark.read.orc('path') -> Optimized Row Columnar
- spark.read.text('path')
- spark.read.format("delta").load('path')
- spark.read.format("sequenceFile").load('path') -> For Hadoop files
- spark.read.format("binaryFile").load('path') -> For Binary files
- spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "table_name").load() -> For SQL Databases

Similarly you can write your Dataframe into different file types such as: 
- df.write.csv('path')
- df.write.parquet('path')
- df.write.json('path')
- df.write.format("avro").save('path')
- df.write.orc('path')
- df.write.text('path')
- df.write.format("delta").save('path')
- df.write.format("sequenceFile").save('path')
- df.write.format("jdbc").option("url", jdbc_url).option("dbtable", "table_name").save()

### Create RDD

```python
rdd = spark.sparkContext.parallelize(df)
```

RDD's are the lowest level of abstraction in Spark.

### Use SQL

```python
spark.sql("SELECT * FROM table")
```

If you are a developer with SQL familiarity, PySpark allows you to use your SQL skills directly without any alteration and along with some additional features.

## To Display Data

If you are using Databricks Notebooks, you can utilize this feature.
```python
display(df)
```

Otherwise, you can use this.
```python
df.show()
```
By default it will only show 10 characters per column. If you wish to see entire content
```python
df.show(truncate=False)
```

### To Print Schema

```python
df.printSchema()
```

We will cover the basic Dataframe operations in our next blog. Stay tuned for more Data Engineering and Spark updates :)