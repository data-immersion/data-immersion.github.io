---
title: "RDD vs Dataframe"
date: 2024-09-23 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python
---

# RDD vs Dataframe

### What is RDD
- RDD stands for Resilient Distributed Dataset.
- It is a core abstraction in Apache Spark.
- It represents a distributed collection of data elements that can be processed in parallel across a cluster.
- It provide the foundation for data processing in Spark and are designed to handle large-scale, distributed computations.
- It is useful incase of fault-tolerant

### Features of RDD:

- Resilient
    - RDDs are fault-tolerant. 
    - If a partition of an RDD is lost due to node failure, Spark can recompute it from the original data or lineage (the sequence of transformations that created the RDD).

- Distributed
    - The data in an RDD is partitioned across multiple nodes in the cluster, enabling parallel processing and scalability.

- Lazy Evaluation
    - Transformations on RDDs are not executed immediately. 
    - They are only computed when an action (e.g., collect, count) is called, allowing Spark to optimize the execution plan

- In-Memory Computation
    - RDDs leverage Spark's in-memory computation capabilities, making processing faster compared to traditional disk-based systems like Hadoop MapReduce.

- Immutable
    - After creation, RDDs are read-only. Any transformation on an RDD (e.g., map, filter) creates a new RDD rather than modifying the existing one.

### Operations

Some of the sample RDD operations are

### Transformations

1) map() -> Applies a function to each element.
2) filter() -> Filters elements based on a condition.
3) flatMap() -> Maps elements and flattens the result.

### Actions

1) collect() -> Returns all elements to the driver.
2) count() -> Returns the number of elements.
3) take(n) -> Returns the first n elements.

### Creation of RDDs

```python
# Using parallelize
data = [1, 2, 3, 4]
rdd = spark.sparkContext.parallelize(data)

# Reading from a file
rdd = spark.sparkContext.textFile("path/to/sample/file.txt")
```

### PROS

- Fault-tolerant with lineage.
- Distributed and scalable.
- Supports in-memory computation.
- Flexible for complex processing tasks.

### CONS

- Lack of schema enforcement makes it less optimized for structured data.
- No built-in optimizations like Catalyst (used in DataFrames).
- More verbose code compared to higher-level APIs like DataFrames or Datasets.
- RDDs store data as objects, which can lead to higher memory usage compared to optimized representations in DataFrames or Datasets.

### What is Dataframe

- DataFrame in Apache Spark is a distributed, tabular data abstraction similar to a table in a relational database.
- It provides a high-level API for structured data processing and is part of Spark’s SQL module.
- Dataframes are built on top of RDDs.


### Features of Dataframe:

- Distributed
    - Like RDDs, DataFrames are distributed across the cluster and can process large-scale datasets in parallel.

- Schema Enforcement
    - DataFrames are structured collections of data, where each column has a name and a data type. 
    - The schema ensures consistency and makes it easier to handle structured data.

- SQL-Like Operations
    - You can interact with DataFrames using SQL queries or high-level APIs, making it accessible for users familiar with SQL.

- In-Memory Computation
    - Supports in-memory data storage for faster computation, particularly in iterative algorithms.

### Operations

Some of the sample Dataframe operations are

### Transformations

1) select -> df.select("Name").show()
2) filter -> df.filter(df.Age > 25).show()
3) group by -> df.groupBy("Age").count().show()

### Actions

1) show -> df.show()
2) collect -> data = df.collect()
3) count -> df.count()

### Using Dataframe in SQL Queries

```python
df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people WHERE Age > 18").show()
```

### Creation of Dataframe

```python
# Using list of tuple
data = [("Alice", 18), ("Bob", 19)]
schema = ["Name", "Age"]
df = spark.createDataFrame(data, schema=schema)

# Using file
df = spark.read.csv("path/to/sample/file.csv", header=True, inferSchema=True)

# Using RDD
rdd = spark.sparkContext.parallelize([("Alice", 18), ("Bob", 19)])
df = rdd.toDF(["Name", "Age"])

# From table
df = spark.read.table("catalog_name.schema_name.table_name")

```

### PROS

- Provides a simple and expressive API, making it easier to work with structured data compared to RDDs.
- Allows SQL queries directly on DataFrames, which is useful for users familiar with SQL.
- DataFrames can be created from various formats, including CSV, JSON, Parquet, Avro, ORC, and JDBC.
- DataFrames integrate seamlessly with Spark MLlib for machine learning workflows.

### CONS

- Like RDDs, DataFrames are immutable. Any transformation creates a new DataFrame, which can lead to inefficiency in some cases.
- Low-level operations might be harder to implement compared to RDDs.
