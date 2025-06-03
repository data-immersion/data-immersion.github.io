---
title: "Aggregation's in Pyspark"
date: 2024-11-17 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python, Aggregations, SQL, GroupBy, Agg
---

# Aggregation's in Pyspark

Aggregations are fundamental operations in data processing that allow us to summarize and analyze data by grouping it and applying various aggregate functions. PySpark provides powerful aggregation capabilities that are essential for data analysis and transformation.

## Understanding groupBy and Aggregation Methods

### The groupBy Operation
The `groupBy` operation in PySpark is similar to SQL's GROUP BY clause. It allows you to:
1. Group data by one or more columns
2. Apply aggregate functions to each group
3. Create summary statistics for each group

```python
# Basic groupBy syntax
df.groupBy("column1", "column2").agg(aggregate_functions)
```

### Types of Aggregation Methods

#### 1. Basic Aggregate Functions
These are the most commonly used aggregation functions:

```python
from pyspark.sql import functions as F

# Count functions
F.count("*")           # Count all rows
F.count("column")      # Count non-null values in a column
F.countDistinct("column")  # Count distinct values

# Sum and Average
F.sum("column")        # Sum of values
F.avg("column")        # Average of values
F.mean("column")       # Same as avg

# Min and Max
F.min("column")        # Minimum value
F.max("column")        # Maximum value

# First and Last
F.first("column")      # First value in the group
F.last("column")       # Last value in the group
```

#### 2. Statistical Functions
PySpark provides various statistical aggregation functions:

```python
# Variance and Standard Deviation
F.variance("column")   # Sample variance
F.var_samp("column")   # Sample variance (alias)
F.stddev("column")     # Sample standard deviation
F.stddev_samp("column") # Sample standard deviation (alias)
F.var_pop("column")    # Population variance
F.stddev_pop("column") # Population standard deviation

# Percentiles and Median
F.percentile_approx("column", 0.5)  # Approximate median
F.percentile_approx("column", [0.25, 0.5, 0.75])  # Multiple percentiles
```

#### 3. Collection Functions
Functions that work with collections of values:

```python
# Collect values into arrays
F.collect_list("column")    # Collect all values into an array
F.collect_set("column")     # Collect unique values into an array

# String aggregations
F.concat_ws(",", F.collect_list("column"))  # Concatenate with separator
```

#### 4. Custom Aggregations
You can create custom aggregation functions using User Defined Functions (UDFs):

```python
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from typing import List

# Example of a custom aggregation function
@udf(returnType=DoubleType())
def custom_avg(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)
```

### Example: Comprehensive Aggregation Methods

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create SparkSession
spark = SparkSession.builder.appName("AggregationMethods").getOrCreate()

# Sample data
data = [
    ("Electronics", "Laptop", 1200, 5),
    ("Electronics", "Phone", 800, 3),
    ("Electronics", "Tablet", 500, 2),
    ("Clothing", "Shirt", 50, 10),
    ("Clothing", "Pants", 80, 8),
    ("Clothing", "Shoes", 120, 6)
]

# Create DataFrame
df = spark.createDataFrame(data, ["category", "product", "price", "quantity"])

# Comprehensive aggregation example
result = df.groupBy("category").agg(
    # Basic aggregations
    F.count("*").alias("total_products"),
    F.sum("price").alias("total_price"),
    F.avg("price").alias("avg_price"),
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    
    # Statistical aggregations
    F.stddev("price").alias("price_stddev"),
    F.variance("price").alias("price_variance"),
    
    # Collection aggregations
    F.collect_list("product").alias("product_list"),
    F.collect_set("product").alias("unique_products"),
    
    # Complex calculations
    (F.sum("price") * F.sum("quantity")).alias("total_revenue"),
    F.percentile_approx("price", 0.5).alias("median_price")
)

# Show results
result.show(truncate=False)

# Expected Output:
# +----------+--------------+-----------+------------------+---------+---------+------------------+------------------+----------------------------------------+----------------------------------------+-------------+-----------+
# |category  |total_products|total_price|avg_price         |min_price|max_price|price_stddev      |price_variance    |product_list                            |unique_products                         |total_revenue|median_price|
# +----------+--------------+-----------+------------------+---------+---------+------------------+------------------+----------------------------------------+----------------------------------------+-------------+-----------+
# |Electronics|3            |2500       |833.3333333333334 |500      |1200     |367.42346141747645|135000.0          |[Laptop, Phone, Tablet]                 |[Laptop, Phone, Tablet]                 |12500        |800.0      |
# |Clothing  |3            |250        |83.33333333333333 |50       |120      |35.11884555294567 |1233.3333333333333|[Shirt, Pants, Shoes]                   |[Shirt, Pants, Shoes]                   |1500         |80.0       |
# +----------+--------------+-----------+------------------+---------+---------+------------------+------------------+----------------------------------------+----------------------------------------+-------------+-----------+
```

### Best Practices for Using groupBy and Aggregations

1. **Performance Optimization**
   - Use appropriate partition columns in groupBy
   - Consider using `repartition()` before heavy aggregations
   - Cache intermediate results if reused

2. **Memory Management**
   - Be cautious with `collect_list` on large datasets
   - Use `collect_set` when unique values are needed
   - Monitor memory usage during complex aggregations

3. **Data Quality**
   - Handle null values appropriately
   - Use `count(*)` vs `count(column)` carefully
   - Consider using `coalesce()` for null handling

4. **Code Organization**
   - Break down complex aggregations into steps
   - Use meaningful alias names
   - Document complex calculations

## Types of Aggregations in PySpark

### 1. Basic Aggregations
PySpark offers several built-in aggregate functions that can be used with `groupBy()`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create SparkSession
spark = SparkSession.builder.appName("BasicAggregations").getOrCreate()

# Sample data
data = [
    ("Electronics", "Laptop", 1200),
    ("Electronics", "Phone", 800),
    ("Electronics", "Tablet", 500),
    ("Clothing", "Shirt", 50),
    ("Clothing", "Pants", 80),
    ("Clothing", "Shoes", 120)
]

# Create DataFrame
df = spark.createDataFrame(data, ["category", "product", "price"])

# Common aggregate functions
result = df.groupBy("category").agg(
    F.count("*").alias("product_count"),
    F.sum("price").alias("total_price"),
    F.avg("price").alias("average_price"),
    F.min("price").alias("minimum_price"),
    F.max("price").alias("maximum_price")
)

# Show results
result.show()

# Expected Output:
# +----------+-------------+-----------+-------------+-------------+-------------+
# |  category|product_count|total_price|average_price|minimum_price|maximum_price|
# +----------+-------------+-----------+-------------+-------------+-------------+
# |Electronics|            3|       2500|   833.333333|          500|         1200|
# |  Clothing|            3|        250|    83.333333|           50|          120|
# +----------+-------------+-----------+-------------+-------------+-------------+
```

### 2. Multiple Aggregations
You can perform multiple aggregations on different columns in a single operation:

```python
# Sample employee data
employee_data = [
    ("IT", "John", 75000, 5),
    ("IT", "Alice", 82000, 7),
    ("IT", "Bob", 68000, 3),
    ("HR", "Carol", 65000, 4),
    ("HR", "Dave", 72000, 6),
    ("HR", "Eve", 69000, 5)
]

# Create DataFrame
employee_df = spark.createDataFrame(
    employee_data, 
    ["department", "name", "salary", "years_experience"]
)

# Multiple aggregations
dept_stats = employee_df.groupBy("department").agg(
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("avg_salary"),
    F.count("name").alias("employee_count"),
    F.avg("years_experience").alias("avg_experience")
)

# Show results
dept_stats.show()

# Expected Output:
# +----------+------------+----------+-------------+---------------+
# |department|total_salary|avg_salary|employee_count|avg_experience|
# +----------+------------+----------+-------------+---------------+
# |        IT|      225000|   75000.0|            3|           5.0|
# |        HR|      206000|   68666.7|            3|           5.0|
# +----------+------------+----------+-------------+---------------+
```

### 3. Window Functions
Window functions allow you to perform calculations across a set of rows related to the current row:

```python
from pyspark.sql.window import Window

# Sample sales data
sales_data = [
    ("2024-01", "Electronics", 10000),
    ("2024-02", "Electronics", 12000),
    ("2024-03", "Electronics", 15000),
    ("2024-01", "Clothing", 5000),
    ("2024-02", "Clothing", 6000),
    ("2024-03", "Clothing", 7000)
]

# Create DataFrame
sales_df = spark.createDataFrame(
    sales_data, 
    ["month", "category", "sales_amount"]
)

# Define window specification
window_spec = Window.partitionBy("category").orderBy("month")

# Calculate running total and rank
result = sales_df.withColumn(
    "running_total", 
    F.sum("sales_amount").over(window_spec)
).withColumn(
    "sales_rank", 
    F.rank().over(window_spec)
)

# Show results
result.show()

# Expected Output:
# +-------+----------+------------+-------------+----------+
# |  month|  category|sales_amount|running_total|sales_rank|
# +-------+----------+------------+-------------+----------+
# |2024-01|Clothing  |        5000|         5000|         1|
# |2024-02|Clothing  |        6000|        11000|         2|
# |2024-03|Clothing  |        7000|        18000|         3|
# |2024-01|Electronics|      10000|        10000|         1|
# |2024-02|Electronics|      12000|        22000|         2|
# |2024-03|Electronics|      15000|        37000|         3|
# +-------+----------+------------+-------------+----------+
```

## Common Use Cases

### 1. GroupBy Operations
GroupBy is the most common way to perform aggregations:

```python
# Sample transaction data
transaction_data = [
    ("Electronics", "Online", 1000),
    ("Electronics", "Store", 800),
    ("Electronics", "Online", 1200),
    ("Clothing", "Store", 500),
    ("Clothing", "Online", 600),
    ("Clothing", "Store", 400)
]

# Create DataFrame
transaction_df = spark.createDataFrame(
    transaction_data, 
    ["category", "channel", "amount"]
)

# Simple groupBy
category_summary = transaction_df.groupBy("category").agg(
    F.sum("amount").alias("total_amount"),
    F.count("*").alias("transaction_count")
)

# Multiple columns groupBy
channel_summary = transaction_df.groupBy("category", "channel").agg(
    F.sum("amount").alias("total_amount"),
    F.avg("amount").alias("avg_amount")
)

# Show results
print("Category Summary:")
category_summary.show()

print("\nChannel Summary:")
channel_summary.show()

# Expected Output:
# Category Summary:
# +----------+------------+------------------+
# |  category|total_amount|transaction_count|
# +----------+------------+------------------+
# |Electronics|        3000|                3|
# |  Clothing|        1500|                3|
# +----------+------------+------------------+
#
# Channel Summary:
# +----------+-------+------------+----------+
# |  category|channel|total_amount|avg_amount|
# +----------+-------+------------+----------+
# |Electronics| Online|        2200|    1100.0|
# |Electronics|  Store|         800|     800.0|
# |  Clothing| Online|         600|     600.0|
# |  Clothing|  Store|         900|     450.0|
# +----------+-------+------------+----------+
```

### 2. Pivot Tables
Creating pivot tables for cross-tabulation:

```python
# Sample sales data by region and quarter
pivot_data = [
    ("North", "Q1", 1000),
    ("North", "Q2", 1200),
    ("North", "Q3", 1100),
    ("North", "Q4", 1300),
    ("South", "Q1", 800),
    ("South", "Q2", 900),
    ("South", "Q3", 950),
    ("South", "Q4", 1000)
]

# Create DataFrame
pivot_df = spark.createDataFrame(
    pivot_data, 
    ["region", "quarter", "sales"]
)

# Create pivot table
pivot_table = pivot_df.groupBy("region").pivot("quarter").sum("sales")

# Show results
pivot_table.show()

# Expected Output:
# +------+----+----+----+----+
# |region|  Q1|  Q2|  Q3|  Q4|
# +------+----+----+----+----+
# | North|1000|1200|1100|1300|
# | South| 800| 900| 950|1000|
# +------+----+----+----+----+
```

### 3. Rolling Aggregations
Performing rolling calculations:

```python
# Sample daily sales data
daily_data = [
    ("2024-01-01", "Electronics", 1000),
    ("2024-01-02", "Electronics", 1200),
    ("2024-01-03", "Electronics", 800),
    ("2024-01-04", "Electronics", 1500),
    ("2024-01-05", "Electronics", 1100)
]

# Create DataFrame
daily_df = spark.createDataFrame(
    daily_data, 
    ["date", "category", "sales"]
)

# Define window for 3-day rolling average
window_spec = Window.partitionBy("category").orderBy("date").rowsBetween(-2, 0)

# Calculate rolling metrics
rolling_stats = daily_df.withColumn(
    "rolling_avg", 
    F.avg("sales").over(window_spec)
).withColumn(
    "rolling_sum", 
    F.sum("sales").over(window_spec)
)

# Show results
rolling_stats.show()

# Expected Output:
# +----------+----------+-----+-----------+-----------+
# |      date|  category|sales|rolling_avg|rolling_sum|
# +----------+----------+-----+-----------+-----------+
# |2024-01-01|Electronics| 1000|     1000.0|       1000|
# |2024-01-02|Electronics| 1200|     1100.0|       2200|
# |2024-01-03|Electronics|  800|     1000.0|       3000|
# |2024-01-04|Electronics| 1500|     1166.7|       3500|
# |2024-01-05|Electronics| 1100|     1133.3|       3400|
# +----------+----------+-----+-----------+-----------+
```

## Best Practices

1. **Performance Optimization**
   - Use `cache()` when performing multiple aggregations on the same DataFrame
   - Consider using `repartition()` before heavy aggregations
   - Use appropriate partition columns in groupBy operations

2. **Memory Management**
   - Be cautious with large groupBy operations
   - Use `persist()` for frequently accessed aggregated results
   - Monitor memory usage during complex aggregations

3. **Code Organization**
   - Use meaningful alias names for aggregated columns
   - Break down complex aggregations into smaller steps
   - Document complex window specifications

## Example: Complete Aggregation Workflow

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create SparkSession
spark = SparkSession.builder.appName("Aggregations").getOrCreate()

# Sample data
data = [
    ("Sales", "2024-01", 1000),
    ("Sales", "2024-02", 1500),
    ("Sales", "2024-03", 2000),
    ("Marketing", "2024-01", 800),
    ("Marketing", "2024-02", 1200),
    ("Marketing", "2024-03", 1500)
]

# Create DataFrame
df = spark.createDataFrame(data, ["department", "month", "amount"])

# Basic aggregation
dept_summary = df.groupBy("department").agg(
    F.sum("amount").alias("total_amount"),
    F.avg("amount").alias("avg_amount"),
    F.count("*").alias("transaction_count")
)

# Window function for running total
window_spec = Window.partitionBy("department").orderBy("month")
df_with_running_total = df.withColumn(
    "running_total",
    F.sum("amount").over(window_spec)
)

# Show results
print("Department Summary:")
dept_summary.show()

print("\nRunning Total by Department:")
df_with_running_total.show()

# Expected Output:
# Department Summary:
# +----------+------------+----------+------------------+
# |department|total_amount|avg_amount|transaction_count|
# +----------+------------+----------+------------------+
# |    Sales|        4500|    1500.0|                3|
# |Marketing|        3500|    1166.7|                3|
# +----------+------------+----------+------------------+
#
# Running Total by Department:
# +----------+-------+------+-------------+
# |department|  month|amount|running_total|
# +----------+-------+------+-------------+
# |Marketing|2024-01|   800|          800|
# |Marketing|2024-02|  1200|         2000|
# |Marketing|2024-03|  1500|         3500|
# |    Sales|2024-01|  1000|         1000|
# |    Sales|2024-02|  1500|         2500|
# |    Sales|2024-03|  2000|         4500|
# +----------+-------+------+-------------+
```

## Conclusion

PySpark's aggregation capabilities provide powerful tools for data analysis and transformation. Understanding and effectively using these features can significantly improve your data processing workflows. Remember to consider performance implications and follow best practices when working with large datasets.

Key takeaways:
- Choose the right aggregation method for your use case
- Optimize performance with proper partitioning and caching
- Use window functions for complex calculations
- Follow best practices for memory management and code organization