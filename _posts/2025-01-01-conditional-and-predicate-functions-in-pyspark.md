---
title: "Conditional & Predicate Functions in Pyspark"
date: 2024-11-17 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python, SQL, Conditional, Predicate
---

# Conditional & Predicate Functions in Pyspark

PySpark provides several powerful conditional functions that help in handling null values, NaN values, and implementing conditional logic in your data transformations. Let's explore some of the most commonly used conditional functions.

## 1. `ifnull` Function

The `ifnull` function returns the second argument if the first argument is null, otherwise returns the first argument.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import ifnull

# Example usage
df = spark.createDataFrame([
    (1, None),
    (2, "value"),
    (None, "default")
], ["id", "value"])

# Replace null values with "default"
df = df.withColumn("new_value", ifnull(col("value"), "default"))
```

## 2. `nanvl` Function

The `nanvl` function returns the second argument if the first argument is NaN, otherwise returns the first argument. This is particularly useful when working with floating-point numbers.

```python
from pyspark.sql.functions import nanvl

# Example usage
df = spark.createDataFrame([
    (1.0, float('nan')),
    (2.0, 3.0),
    (float('nan'), 0.0)
], ["value1", "value2"])

# Replace NaN values with 0.0
df = df.withColumn("clean_value", nanvl(col("value1"), 0.0))
```

## 3. `nullif` Function

The `nullif` function returns null if the two arguments are equal, otherwise returns the first argument.

```python
from pyspark.sql.functions import nullif

# Example usage
df = spark.createDataFrame([
    (1, 1),
    (2, 3),
    (4, 4)
], ["value1", "value2"])

# Returns null when value1 equals value2
df = df.withColumn("result", nullif(col("value1"), col("value2")))
```

## 4. `nullifzero` Function

The `nullifzero` function returns null if the argument is 0, otherwise returns the argument.

```python
from pyspark.sql.functions import nullifzero

# Example usage
df = spark.createDataFrame([
    (0,),
    (1,),
    (2,)
], ["value"])

# Returns null for zero values
df = df.withColumn("result", nullifzero(col("value")))
```

## 5. `when` Function

The `when` function is one of the most versatile conditional functions in PySpark. It allows you to implement complex conditional logic using a chain of when-otherwise statements.

```python
from pyspark.sql.functions import when

# Example usage
df = spark.createDataFrame([
    (1, "A"),
    (2, "B"),
    (3, "C")
], ["id", "category"])

# Complex conditional logic
df = df.withColumn("status",
    when(col("id") < 2, "low")
    .when(col("id") < 3, "medium")
    .otherwise("high")
)
```

## Best Practices

1. Always import the required functions from `pyspark.sql.functions`
2. Use `when` for complex conditional logic as it's more readable than nested if-else statements
3. Consider using `ifnull` and `nanvl` for handling null and NaN values respectively
4. Use `nullif` when you need to convert specific values to null
5. Remember that these functions are evaluated lazily and are optimized by Spark's query optimizer

## Common Use Cases

1. Data cleaning and standardization
2. Handling missing values
3. Implementing business logic in data transformations
4. Creating derived columns based on conditions
5. Data validation and quality checks

These conditional functions are essential tools in your PySpark data transformation toolkit. They help you write more concise and efficient code while maintaining readability and performance.

Also, here we covered important functions, there are other functions similar to it - [please take look to find additional functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#conditional-functions)

# Predicate Functions in PySpark

Predicate functions in PySpark are used to evaluate conditions and return boolean values. These functions are essential for filtering data, creating conditional expressions, and data validation. Let's explore the commonly used predicate functions.

## 1. `equal_null` Function

The `equal_null` function compares two expressions and returns true if they are equal, including when both are null. This is different from the regular equality operator (`==`) which returns null when comparing null values.

```python
from pyspark.sql.functions import equal_null

# Example usage
df = spark.createDataFrame([
    (1, 1),
    (None, None),
    (2, 3)
], ["value1", "value2"])

# Compare values including null equality
df = df.withColumn("are_equal", equal_null(col("value1"), col("value2")))
```

## 2. `ilike` Function

The `ilike` function performs case-insensitive pattern matching. It's similar to SQL's ILIKE operator.

```python
from pyspark.sql.functions import ilike

# Example usage
df = spark.createDataFrame([
    ("Hello",),
    ("HELLO",),
    ("hello",),
    ("Goodbye",)
], ["text"])

# Case-insensitive pattern matching
df = df.filter(ilike(col("text"), "hello"))
```

## 3. `isnan` Function

The `isnan` function checks if a value is NaN (Not a Number). This is particularly useful when working with floating-point numbers.

```python
from pyspark.sql.functions import isnan

# Example usage
df = spark.createDataFrame([
    (float('nan'),),
    (1.0,),
    (float('inf'),)
], ["value"])

# Filter NaN values
df = df.filter(isnan(col("value")))
```

## 4. `isnotnull` and `isnull` Functions

These functions check if a value is not null or is null, respectively.

```python
from pyspark.sql.functions import isnotnull, isnull

# Example usage
df = spark.createDataFrame([
    (1,),
    (None,),
    (2,)
], ["value"])

# Filter non-null values
df_not_null = df.filter(isnotnull(col("value")))

# Filter null values
df_null = df.filter(isnull(col("value")))
```

## 5. `like` Function

The `like` function performs case-sensitive pattern matching using SQL LIKE syntax. It supports wildcards:
- `%` matches any sequence of characters
- `_` matches any single character

```python
from pyspark.sql.functions import like

# Example usage
df = spark.createDataFrame([
    ("Hello World",),
    ("Hello there",),
    ("Goodbye World",)
], ["text"])

# Pattern matching with wildcards
df = df.filter(like(col("text"), "Hello%"))
```

## 6. `regexp`, `rlike`, and `regexp_like` Functions

These functions perform pattern matching using regular expressions. They are aliases of each other and provide the same functionality.

```python
from pyspark.sql.functions import regexp

# Example usage
df = spark.createDataFrame([
    ("abc123",),
    ("def456",),
    ("ghi789",)
], ["text"])

# Regular expression pattern matching
df = df.filter(regexp(col("text"), "^[a-z]{3}\\d{3}$"))
```

## Best Practices for Predicate Functions

1. Use `equal_null` when you need to compare values including null equality
2. Prefer `ilike` over `like` when case-insensitive matching is required
3. Use `isnan` specifically for checking NaN values in floating-point columns
4. Use `isnull` and `isnotnull` for null checks instead of comparing with None
5. Use regular expressions (`regexp`, `rlike`, `regexp_like`) for complex pattern matching
6. Consider performance implications when using regular expressions in large datasets

## Common Use Cases

1. Data validation and cleaning
2. Filtering datasets based on conditions
3. Pattern matching in text data
4. Null value handling
5. Data quality checks
6. Complex conditional logic in data transformations

Predicate functions are powerful tools for data filtering and validation in PySpark. They provide a rich set of operations for working with null values, pattern matching, and conditional logic. Understanding these functions is crucial for effective data processing and transformation in Spark applications.

Also, here we covered important functions, there are other functions similar to it - [please take look to find additional functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#predicate-functions)