---
title: "Transformations vs Actions"
date: 2024-10-10 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python
---

# Transformations vs Actions

### What is Transformations?
- Transformations are operations that produce a new RDD (Resilient Distributed Dataset) or DataFrame by applying a set of functions to an existing RDD or DataFrame.
- They are the building blocks for creating the data processing pipeline in Spark.
- They are lazy (i.e)., they do not execute immediately but build up a logical execution plan.
- They are only executed when an action is invoked, triggering the computation.
- Transformations do not modify the original RDD or DataFrame. Instead, they return a new RDD or DataFrame with the transformed data.

### Transformations in RDD

- map: Applies a function to each element -> rdd.map(lambda x: x * 2)
- filter: Filters elements based on a condition -> rdd.filter(lambda x: x > 10)
- flatMap: Breaks each element into multiple elements -> rdd.flatMap(lambda x: x.split(" "))
- reduceByKey: Aggregates data with the same key -> rdd.reduceByKey(lambda x, y: x + y)

### Transformations in Dataframe

- select: Selects specific columns -> df.select("column1", "column2")
- filter / where: Filters rows based on a condition -> df.filter(df["age"] > 18)
- withColumn: Adds or modifies a column -> df.withColumn("new_column", df["existing_column"] + 1)
- join: Combines two DataFrames based on a key -> df1.join(df2, df1["id"] == df2["id"], "inner")

### Benefits of Transformations

- **Performance Optimization**: Spark optimizes the DAG of transformations before execution.
- **Immutability**: Ensures data integrity by not modifying the original dataset.
- **Flexibility**: Supports a wide range of operations for data manipulation

### Sample Implementation

```python
# RDD example
rdd = sc.parallelize([1, 2, 3, 4])
transformed_rdd = rdd.map(lambda x: x * 2)  # Transformation (lazy)
print(transformed_rdd)  # No output yet
print(transformed_rdd.collect()) # Output: [2, 4, 6, 8] (action triggers execution)

# Dataframe Example
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
transformed_df = df.filter(df["id"] > 1)  # Transformation (lazy)
transformed_df.show()  # Action triggers execution, displays rows
```

### What is Actions?
- Actions are operations that trigger the execution of the transformations defined on RDDs (Resilient Distributed Datasets) or DataFrames. 
- Actions produce a result, either by returning data to the driver program or by writing data to external storage.
- They execute immediately.
- Unlike transformations, actions cause the execution of the Spark job. They compute the result by executing the Directed Acyclic Graph (DAG) of transformations.

### Actions in RDD:

- collect: Returns all elements of the RDD to the driver -> rdd.collect()
- count: Counts the number of elements in the RDD -> rdd.count()
- first: Returns the first element of the RDD -> rdd.first()
- take: Returns the first n elements -> rdd.take(3)
- saveAsTextFile: Saves the RDD data to a text file -> rdd.saveAsTextFile("/path/to/output")

### Actions in Dataframe:

- show: Displays rows from the DataFrame -> df.show()
- count: Counts the number of rows in the DataFrame -> df.count()
- collect: Returns all rows as a list of Row objects -> df.collect()
- toPandas: Converts the DataFrame into a Pandas DataFrame -> pandas_df = df.toPandas()
- write: Saves the DataFrame to an external storage location -> df.write.csv("/path/to/output")

### Sample Implementation

```python

# RDD example

rdd = sc.parallelize([1, 2, 3, 4])
# Transformation: defines a computation (lazy)
transformed_rdd = rdd.map(lambda x: x * 2)
# Action: triggers the computation and returns the result
result = transformed_rdd.collect()
print(result)  # Output: [2, 4, 6, 8]

# Dataframe example
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
# Action: triggers execution and displays rows
df.show()
```



