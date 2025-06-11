---
title: "Pyspark.sql.functions"
date: 2024-11-17 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python, SQL, Functions, Col, lit
---

# Pyspark.sql.functions

PySpark SQL functions provide a rich set of built-in functions for data manipulation and transformation. These functions are essential for data processing in Spark applications. Let's explore some of the most commonly used functions:

## Basic Column Operations

### col()
The `col()` function is used to create a Column object from a column name.

```python
from pyspark.sql.functions import col

# Example
df.select(col("name"), col("age"))
```

### lit()
The `lit()` function creates a Column of literal value.

```python
from pyspark.sql.functions import lit

# Example
df.select(col("name"), lit("constant_value").alias("constant_column"))
```

## Array Functions

### array()
Creates a new array column.

```python
from pyspark.sql.functions import array

# Example
df.select(array(col("col1"), col("col2")).alias("new_array"))
```

### array_contains()
Checks if an array contains a value.

```python
from pyspark.sql.functions import array_contains

# Example
df.select(array_contains(col("array_column"), "value"))
```

### explode()
Explodes an array or map column into multiple rows.

```python
from pyspark.sql.functions import explode

# Example
df.select(explode(col("array_column")))
```

## Map Functions

### map_keys()
Returns an array of keys from a map column.

```python
from pyspark.sql.functions import map_keys

# Example
df.select(map_keys(col("map_column")))
```

### map_values()
Returns an array of values from a map column.

```python
from pyspark.sql.functions import map_values

# Example
df.select(map_values(col("map_column")))
```

## JSON Functions

### json_tuple()
Extracts JSON fields into columns.

```python
from pyspark.sql.functions import json_tuple

# Example
df.select(json_tuple(col("json_column"), "field1", "field2"))
```

### get_json_object()
Extracts a JSON object from a JSON string.

```python
from pyspark.sql.functions import get_json_object

# Example
df.select(get_json_object(col("json_column"), "$.field"))
```

### to_json()
Converts a struct/map/array to a JSON string.

```python
from pyspark.sql.functions import to_json

# Example
df.select(to_json(col("struct_column")))
```

### from_json()
Parses a JSON string into a struct/map/array.

```python
from pyspark.sql.functions import from_json

# Example
schema = "struct<name:string,age:int>"
df.select(from_json(col("json_string"), schema))
```

## String Functions

### concat()
Concatenates multiple string columns.

```python
from pyspark.sql.functions import concat

# Example
df.select(concat(col("first_name"), lit(" "), col("last_name")))
```

### split()
Splits a string into an array.

```python
from pyspark.sql.functions import split

# Example
df.select(split(col("string_column"), ","))
```

## Date/Time Functions

### current_date()
Returns the current date.

```python
from pyspark.sql.functions import current_date

# Example
df.select(current_date())
```

### date_format()
Formats a date column.

```python
from pyspark.sql.functions import date_format

# Example
df.select(date_format(col("date_column"), "yyyy-MM-dd"))
```

## Aggregation Functions

### count()
Counts the number of rows.

```python
from pyspark.sql.functions import count

# Example
df.select(count("*"))
```

### sum()
Calculates the sum of a column.

```python
from pyspark.sql.functions import sum

# Example
df.select(sum(col("amount")))
```

### avg()
Calculates the average of a column.

```python
from pyspark.sql.functions import avg

# Example
df.select(avg(col("score")))
```

## Window Functions

### row_number()
Assigns a unique row number to each row.

```python
from pyspark.sql.functions import row_number
from pyspark.sql import Window

# Example
window_spec = Window.partitionBy("department").orderBy("salary")
df.select(row_number().over(window_spec))
```

### rank()
Assigns a rank to each row.

```python
from pyspark.sql.functions import rank
from pyspark.sql import Window

# Example
window_spec = Window.partitionBy("department").orderBy("salary")
df.select(rank().over(window_spec))
```

## Type Conversion Functions

### cast()
Casts a column to a different data type.

```python
from pyspark.sql.functions import col

# Example
df.select(col("string_column").cast("integer"))
```

### to_date()
Converts a string to a date.

```python
from pyspark.sql.functions import to_date

# Example
df.select(to_date(col("date_string"), "yyyy-MM-dd"))
```

## Expression and Conditional Functions

### expr()
The `expr()` function allows you to write SQL-like expressions as strings. This is particularly useful for complex expressions or when you want to use SQL syntax directly.

```python
from pyspark.sql.functions import expr

# Example 1: Simple arithmetic
df.select(expr("salary * 1.1 as increased_salary"))

# Example 2: Complex conditions
df.select(expr("CASE WHEN age > 18 THEN 'Adult' ELSE 'Minor' END as age_category"))

# Example 3: String operations
df.select(expr("CONCAT(first_name, ' ', last_name) as full_name"))

# Example 4: Date operations
df.select(expr("DATE_ADD(birth_date, 365) as next_birthday"))
```

### coalesce()
The `coalesce()` function returns the first non-null value from a list of columns. It's particularly useful for handling null values and providing fallback values.

```python
from pyspark.sql.functions import coalesce

# Example 1: Basic usage with multiple columns
df.select(coalesce(col("primary_phone"), col("secondary_phone"), lit("No phone")))

# Example 2: With nested conditions
df.select(
    coalesce(
        col("preferred_name"),
        col("nickname"),
        col("first_name"),
        lit("Unknown")
    ).alias("display_name")
)

# Example 3: In aggregation
df.groupBy("department").agg(
    coalesce(
        sum(col("bonus")),
        lit(0)
    ).alias("total_bonus")
)
```

## Best Practices

1. Always import functions from `pyspark.sql.functions` at the beginning of your script
2. Use type hints when possible for better code readability
3. Chain transformations for better performance
4. Use appropriate data types for columns
5. Consider using UDFs (User Defined Functions) for complex transformations

## Performance Considerations

1. Avoid using UDFs when possible as they can impact performance
2. Use built-in functions instead of custom implementations
3. Consider partitioning and bucketing for large datasets
4. Use appropriate data types to minimize memory usage
5. Leverage Spark's optimization features like predicate pushdown

Also, here we covered important functions, there are other functions similar to it - [please take look to find additional functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#normal-functions)

