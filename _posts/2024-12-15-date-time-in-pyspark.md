---
title: "Date Time in Pyspark"
date: 2024-12-15 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python, DateTime
---

# Date Time in Pyspark

## Understanding Date and Time in PySpark

Before diving into the functions, let's understand some key concepts about date and time handling in PySpark:

### Date Types in PySpark
PySpark supports several date and time data types:
- `DateType`: Represents a date without time (e.g., 2024-03-15)
- `TimestampType`: Represents a date with time (e.g., 2024-03-15 14:30:00)
- `StringType`: When dates are stored as strings (e.g., "2024-03-15")

### Timezone Handling
PySpark handles timezones in the following ways:
- By default, timestamps are stored in UTC
- Timezone conversions can be performed using `from_utc_timestamp` and `to_utc_timestamp`
- When working with timestamps, always be aware of the timezone context

### Date Format Patterns
PySpark uses Java's date format patterns, where:
- `yyyy`: 4-digit year
- `MM`: 2-digit month
- `dd`: 2-digit day
- `HH`: 24-hour format
- `mm`: minutes
- `ss`: seconds
- `SSS`: milliseconds

### Performance Considerations
- Date operations in PySpark are optimized for distributed processing
- Avoid using Python's datetime library for large datasets
- Use built-in PySpark functions for better performance
- Consider partitioning data by date for better query performance

### Common Challenges
1. **Date Parsing**: Different date formats in source data
2. **Timezone Conversions**: Handling data across different timezones
3. **Date Arithmetic**: Calculating differences and adding/subtracting time periods
4. **Date Validation**: Ensuring dates are valid and in the correct format
5. **Performance**: Optimizing date operations for large datasets

### Best Practices for Date Handling
1. **Consistency**: Use consistent date formats throughout your application
2. **Validation**: Always validate date inputs
3. **Timezone Awareness**: Be explicit about timezone handling
4. **Performance**: Use appropriate date functions for your use case
5. **Documentation**: Document date format patterns and timezone handling

Working with dates and times is a common task in data processing. PySpark provides a rich set of functions to handle date and time operations. In this post, we'll explore various date/time functions available in PySpark with examples.

## Basic Date Functions

### 1. Current Date and Time
```python
from pyspark.sql import functions as F

# Get current date
df = df.withColumn("current_date", F.current_date())

# Get current timestamp
df = df.withColumn("current_timestamp", F.current_timestamp())
```

**Function Explanations:**
- `current_date()`: Returns the current date as a DateType. No parameters required.
- `current_timestamp()`: Returns the current timestamp as a TimestampType. No parameters required.

### 1.1 Timestamp Functions
```python
# Get current timestamp
df = df.withColumn("current_ts", F.current_timestamp())

# Convert string to timestamp
df = df.withColumn("timestamp_col", F.to_timestamp("string_timestamp_col", "yyyy-MM-dd HH:mm:ss"))

# Convert string to timestamp with timezone
df = df.withColumn("timestamp_with_tz", F.to_timestamp("string_timestamp_col", "yyyy-MM-dd HH:mm:ss z"))
```

**Function Explanations:**
- `current_timestamp()`: 
  - Returns the current timestamp as a TimestampType
  - The timestamp is in UTC timezone
  - No parameters required
  - Returns timestamp in format: "yyyy-MM-dd HH:mm:ss.SSS"
  - Example output: "2024-03-15 14:30:00.123"

- `to_timestamp(col, format)`:
  - Converts a string column to a timestamp type
  - Parameters:
    - `col`: The string column to convert
    - `format`: The timestamp format pattern (optional, defaults to "yyyy-MM-dd HH:mm:ss")
  - Common format patterns:
    - `yyyy-MM-dd HH:mm:ss`: "2024-03-15 14:30:00"
    - `yyyy-MM-dd HH:mm:ss.SSS`: "2024-03-15 14:30:00.123"
    - `yyyy-MM-dd'T'HH:mm:ss.SSSZ`: "2024-03-15T14:30:00.123+0000"
    - `yyyy-MM-dd HH:mm:ss z`: "2024-03-15 14:30:00 UTC"
  - Returns null if the string cannot be parsed according to the format
  - Timezone handling:
    - If timezone is specified in the format (e.g., 'z' or 'Z'), the timestamp will be converted to UTC
    - If no timezone is specified, the timestamp is assumed to be in the local timezone

**Example Usage:**
```python
# Different timestamp formats
df = df.withColumn("ts1", F.to_timestamp("date_str", "yyyy-MM-dd"))
df = df.withColumn("ts2", F.to_timestamp("datetime_str", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("ts3", F.to_timestamp("iso_str", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

# With timezone
df = df.withColumn("ts_with_tz", F.to_timestamp("datetime_with_tz", "yyyy-MM-dd HH:mm:ss z"))

# Default format
df = df.withColumn("ts_default", F.to_timestamp("datetime_str"))  # Uses default format
```

**Best Practices:**
1. Always specify the format when converting strings to timestamps to avoid ambiguity
2. Be explicit about timezone handling in your data
3. Use consistent timestamp formats throughout your application
4. Consider using ISO 8601 format for maximum compatibility
5. Handle null values appropriately when converting timestamps

### 2. Converting String to Date
```python
# Using to_date
df = df.withColumn("date_col", F.to_date("string_date_col", "yyyy-MM-dd"))

# Using date_format
df = df.withColumn("formatted_date", F.date_format("date_col", "yyyy-MM-dd"))
```

**Function Explanations:**
- `to_date(col, format)`: Converts a string column to a date type.
  - `col`: The string column to convert
  - `format`: The date format pattern (e.g., "yyyy-MM-dd")
- `date_format(col, format)`: Formats a date/timestamp to a string with the given format.
  - `col`: The date/timestamp column to format
  - `format`: The output format pattern

### 3. Date Arithmetic
```python
# Add days to a date
df = df.withColumn("date_plus_10", F.date_add("date_col", 10))

# Subtract days from a date
df = df.withColumn("date_minus_10", F.date_sub("date_col", 10))

# Add months to a date
df = df.withColumn("date_plus_months", F.add_months("date_col", 3))

# Calculate months between two dates
df = df.withColumn("months_diff", F.months_between("end_date", "start_date"))
```

**Function Explanations:**
- `date_add(col, days)`: Adds a number of days to a date.
  - `col`: The date column
  - `days`: Number of days to add (can be negative)
- `date_sub(col, days)`: Subtracts a number of days from a date.
  - `col`: The date column
  - `days`: Number of days to subtract
- `add_months(col, months)`: Adds a number of months to a date.
  - `col`: The date column
  - `months`: Number of months to add (can be negative)
- `months_between(end, start)`: Calculates the number of months between two dates.
  - `end`: End date
  - `start`: Start date

### 4. Date Components
```python
# Extract year
df = df.withColumn("year", F.year("date_col"))

# Extract month
df = df.withColumn("month", F.month("date_col"))

# Extract day
df = df.withColumn("day", F.dayofmonth("date_col"))

# Extract day of week (1-7, where 1 is Sunday)
df = df.withColumn("day_of_week", F.dayofweek("date_col"))

# Extract quarter
df = df.withColumn("quarter", F.quarter("date_col"))
```

**Function Explanations:**
- `year(col)`: Extracts the year from a date/timestamp.
  - `col`: The date/timestamp column
- `month(col)`: Extracts the month (1-12) from a date/timestamp.
  - `col`: The date/timestamp column
- `dayofmonth(col)`: Extracts the day of the month (1-31).
  - `col`: The date/timestamp column
- `dayofweek(col)`: Extracts the day of the week (1-7, Sunday=1).
  - `col`: The date/timestamp column
- `quarter(col)`: Extracts the quarter (1-4) from a date/timestamp.
  - `col`: The date/timestamp column

### 5. Date Truncation
```python
# Truncate to year
df = df.withColumn("trunc_year", F.trunc("date_col", "year"))

# Truncate to month
df = df.withColumn("trunc_month", F.trunc("date_col", "month"))

# Truncate to week
df = df.withColumn("trunc_week", F.trunc("date_col", "week"))
```

**Function Explanations:**
- `trunc(col, format)`: Truncates a date to the specified unit.
  - `col`: The date/timestamp column
  - `format`: The truncation unit ("year", "month", "week", "day", "hour", "minute", "second")

### 6. Date Difference
```python
# Calculate days between two dates
df = df.withColumn("days_diff", F.datediff("end_date", "start_date"))
```

**Function Explanations:**
- `datediff(end, start)`: Calculates the number of days between two dates.
  - `end`: End date
  - `start`: Start date

### 7. Date Formatting
```python
# Format date in different patterns
df = df.withColumn("formatted_date_1", F.date_format("date_col", "yyyy-MM-dd"))
df = df.withColumn("formatted_date_2", F.date_format("date_col", "dd/MM/yyyy"))
df = df.withColumn("formatted_date_3", F.date_format("date_col", "MMM dd, yyyy"))
```

**Function Explanations:**
- `date_format(col, format)`: Formats a date/timestamp to a string with the given format.
  - `col`: The date/timestamp column
  - `format`: The output format pattern

### 8. Unix Timestamp Operations
```python
# Convert to unix timestamp
df = df.withColumn("unix_timestamp", F.unix_timestamp("date_col"))

# Convert from unix timestamp
df = df.withColumn("from_unix", F.from_unixtime("unix_timestamp"))
```

**Function Explanations:**
- `unix_timestamp(col, format)`: Converts a date/timestamp to Unix timestamp (seconds since 1970-01-01).
  - `col`: The date/timestamp column
  - `format`: Optional format for string input
- `from_unixtime(col, format)`: Converts Unix timestamp to a string with the given format.
  - `col`: The Unix timestamp column
  - `format`: Optional output format (default: "yyyy-MM-dd HH:mm:ss")

### 9. Last Day of Month
```python
# Get last day of the month
df = df.withColumn("last_day", F.last_day("date_col"))
```

**Function Explanations:**
- `last_day(col)`: Returns the last day of the month for the given date.
  - `col`: The date column

### 10. Next Day
```python
# Get next day of the week
df = df.withColumn("next_monday", F.next_day("date_col", "Monday"))
```

**Function Explanations:**
- `next_day(col, dayOfWeek)`: Returns the first date which is later than the value of the date column and is on the specified day of the week.
  - `col`: The date column
  - `dayOfWeek`: Day of the week (case-insensitive, e.g., "Monday", "monday", "MONDAY")

## Common Date Patterns
Here are some commonly used date patterns in PySpark:

- `yyyy-MM-dd`: 2024-03-15
- `dd/MM/yyyy`: 15/03/2024
- `MMM dd, yyyy`: Mar 15, 2024
- `yyyy-MM-dd HH:mm:ss`: 2024-03-15 14:30:00
- `yyyy-MM-dd'T'HH:mm:ss.SSSZ`: 2024-03-15T14:30:00.000+0000

## Best Practices

1. Always specify the date format when converting strings to dates
2. Use appropriate date functions based on your timezone requirements
3. Consider using `current_timestamp()` for audit columns
4. Be careful with timezone conversions
5. Use `date_format()` for consistent date string formatting

## Additional Tips

- PySpark's date functions are based on Java's date/time API
- All date functions return a Column type
- Date operations are null-safe
- Consider using `lit()` function when working with literal dates
- Use `cast()` function to convert between date types

## Example Use Cases

### 1. Age Calculation
```python
df = df.withColumn("age", 
    F.floor(F.months_between(F.current_date(), "birth_date") / 12))
```

### 2. Fiscal Year Calculation
```python
df = df.withColumn("fiscal_year",
    F.when(F.month("date_col") >= 4,
        F.year("date_col") + 1)
    .otherwise(F.year("date_col")))
```

### 3. Business Days Calculation
```python
# Assuming you have a calendar table with business days
df = df.join(calendar_table, 
    (df.date_col >= calendar_table.start_date) & 
    (df.date_col <= calendar_table.end_date))
```

These functions should cover most of your date/time manipulation needs in PySpark. Remember that PySpark's date functions are optimized for distributed processing, so they're generally more efficient than using Python's datetime library for large datasets.
Also, here we covered important functions, there are other functions similar to it - [please take look to find additional functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#date-and-timestamp-functions)