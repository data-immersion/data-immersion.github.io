---
title: "Narrow vs Wide Transformations"
date: 2024-10-20 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python
---

# Narrow vs Wide Transformations

### What is Narrow Transformations?
- Narrow transformations are operations where each input partition contributes to only one output partition.
- This means that the transformation does not require data to be shuffled across the network.
- As a result, narrow transformations are more efficient because they allow data to be processed locally, without the overhead of data movement.

### Advantages of Narrow Transformations
- Better Performance -> Since data doesn't need to be shuffled between partitions, narrow transformations execute faster
- Memory Efficient -> Each partition can be processed independently, requiring less memory overhead
- Fault Tolerance -> If a partition fails, only that specific partition needs to be recomputed
- Pipeline Optimization -> Multiple narrow transformations can be pipelined together for better efficiency
- Reduced Network Traffic -> No data movement between nodes means less network congestion

### Working of Narrow Transformations
- In narrow transformations, each partition of the parent RDD is used by at most one partition of the child RDD.
- The data processing happens locally within each partition.
- Examples include:
    - map
    - filter 
    - flatMap
    - sample

### Sample Implementation

```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NarrowTransformations") \
    .getOrCreate()

# Create a sample RDD
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = spark.sparkContext.parallelize(data, numSlices=4)

# 1. map() transformation
# Applies a function to each element
doubled_rdd = rdd.map(lambda x: x * 2)
print("After map():", doubled_rdd.collect())  # [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

# 2. filter() transformation
# Keeps only elements that satisfy a condition
even_numbers = rdd.filter(lambda x: x % 2 == 0)
print("After filter():", even_numbers.collect())  # [2, 4, 6, 8, 10]

# 3. flatMap() transformation
# Similar to map but can return multiple elements for each input
words = ["hello world", "spark is awesome"]
words_rdd = spark.sparkContext.parallelize(words)
word_list = words_rdd.flatMap(lambda x: x.split())
print("After flatMap():", word_list.collect())  # ['hello', 'world', 'spark', 'is', 'awesome']

# 4. sample() transformation
# Randomly samples elements from the RDD
sampled_rdd = rdd.sample(withReplacement=False, fraction=0.5, seed=42)
print("After sample():", sampled_rdd.collect())  # Random subset of elements

# Stop Spark Session
spark.stop()
```

In this example:
- We create a SparkSession and initialize an RDD with 10 numbers
- Each transformation (map, filter, flatMap, sample) is a narrow transformation because:
  - They process data locally within each partition
  - No data shuffling is required
  - Each input partition contributes to at most one output partition
- The code demonstrates how each transformation works and its output
- All operations maintain the narrow transformation property of processing data locally


### What is Wide Transformations?

- Wide transformations are operations where each input partition may contribute to multiple output partitions
- These transformations require data to be shuffled across the network
- They are also known as "shuffle transformations" because they involve redistributing data across partitions
- Wide transformations are more expensive in terms of performance compared to narrow transformations

### Advantages of Wide Transformations
- Data Shuffling -> Requires data movement between partitions across the network
- Higher Resource Usage -> Consumes more memory and network bandwidth
- Slower Execution -> Due to the overhead of data shuffling
- Potential Bottlenecks -> Can create performance bottlenecks in the Spark job
- Dependency Tracking -> Creates complex dependency chains that need to be tracked

### Common Wide Transformations
- groupByKey() -> Groups values by key, requiring data shuffling
- reduceByKey() -> Aggregates values for each key
- sortBy() -> Sorts RDD by key
- join() -> Combines two RDDs based on keys
- distinct() -> Removes duplicates
- repartition() -> Changes the number of partitions
- coalesce() -> Reduces the number of partitions

### Sample Implementation

```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WideTransformations") \
    .getOrCreate()

# Create sample RDDs
data1 = [("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5)]
data2 = [("a", "x"), ("b", "y"), ("c", "z")]
rdd1 = spark.sparkContext.parallelize(data1)
rdd2 = spark.sparkContext.parallelize(data2)

# 1. groupByKey() transformation
grouped = rdd1.groupByKey()
print("After groupByKey():", grouped.mapValues(list).collect())
# Output: [('a', [1, 3]), ('b', [2, 5]), ('c', [4])]

# 2. reduceByKey() transformation
reduced = rdd1.reduceByKey(lambda x, y: x + y)
print("After reduceByKey():", reduced.collect())
# Output: [('a', 4), ('b', 7), ('c', 4)]

# 3. join() transformation
joined = rdd1.join(rdd2)
print("After join():", joined.collect())
# Output: [('a', (1, 'x')), ('a', (3, 'x')), ('b', (2, 'y')), ('b', (5, 'y')), ('c', (4, 'z'))]

# 4. distinct() transformation
distinct_rdd = rdd1.distinct()
print("After distinct():", distinct_rdd.collect())
# Output: [('a', 1), ('a', 3), ('b', 2), ('b', 5), ('c', 4)]

# Stop Spark Session
spark.stop()
```

In this example:
- We demonstrate several wide transformations that require data shuffling
- Each operation shows how data is redistributed across partitions
- The code includes common use cases for wide transformations
- All operations require network communication between partitions
- The output shows how data is reorganized after each transformation
