---
title: "Let's Start Pyspark"
date: 2024-10-25 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python
---

# Let's Start Pyspark

### Intro
Now you are familiar with pyspark concepts, now its time to experiment with it. 

Since we already covered Data Reader and Writer, now we will see how to work with data.

### Filter

The filter transformation is a narrow transformation that selects elements from an RDD based on a predicate function. It returns a new RDD containing only the elements that satisfy the given condition.

#### Key Characteristics of Filter
- Narrow Transformation -> No data shuffling required
- Lazy Evaluation -> Only executed when an action is called
- Preserves Partitioning -> Maintains the same number of partitions as the input RDD
- Memory Efficient -> Only keeps elements that match the condition

#### Common Use Cases
- Data Cleaning -> Removing null values or invalid records
- Data Filtering -> Selecting specific records based on conditions
- Data Validation -> Keeping only records that meet certain criteria
- Data Sampling -> Creating subsets of data based on conditions

#### Sample Implementation

```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilterTransformation") \
    .getOrCreate()

# Create sample data
data = [
    ("John", 25, "New York"),
    ("Alice", 30, "London"),
    ("Bob", 17, "Paris"),
    ("Emma", 22, "Tokyo"),
    ("Mike", 35, "Berlin")
]

# Create RDD
rdd = spark.sparkContext.parallelize(data)

# 1. Basic filtering by age
adults = rdd.filter(lambda x: x[1] >= 18)
print("Adults only:", adults.collect())
# Output: [('John', 25, 'New York'), ('Alice', 30, 'London'), ('Emma', 22, 'Tokyo'), ('Mike', 35, 'Berlin')]

# 2. Multiple conditions
young_adults = rdd.filter(lambda x: x[1] >= 18 and x[1] <= 30)
print("Young adults:", young_adults.collect())
# Output: [('John', 25, 'New York'), ('Alice', 30, 'London'), ('Emma', 22, 'Tokyo')]

# 3. Filtering with string operations
london_residents = rdd.filter(lambda x: x[2] == "London")
print("London residents:", london_residents.collect())
# Output: [('Alice', 30, 'London')]

# 4. Chaining multiple filters
filtered_data = rdd.filter(lambda x: x[1] >= 18) \
                  .filter(lambda x: len(x[0]) > 3)
print("Adults with names longer than 3 characters:", filtered_data.collect())
# Output: [('John', 25, 'New York'), ('Alice', 30, 'London'), ('Emma', 22, 'Tokyo'), ('Mike', 35, 'Berlin')]

# Stop Spark Session
spark.stop()
```

#### Best Practices
1. Combine Multiple Conditions -> Use a single filter with multiple conditions instead of chaining multiple filters
2. Avoid Complex Logic -> Keep filter conditions simple and readable
3. Consider Data Skew -> Be aware that filtering might create uneven partition sizes
4. Use with Other Transformations -> Often used in combination with map, flatMap, or other transformations
5. Performance Optimization -> Filter early in the transformation chain to reduce data volume

#### Performance Considerations
- Filter is a narrow transformation, so it's generally efficient
- The performance depends on the complexity of the filter condition
- Filtering early in the transformation chain can improve overall performance
- Consider the selectivity of the filter condition (how many records it will keep)

### Select

The select transformation is a narrow transformation that allows you to choose specific columns from a DataFrame. It's one of the most commonly used transformations in PySpark for data manipulation and analysis.

#### Key Characteristics of Select
- Narrow Transformation -> No data shuffling required
- Lazy Evaluation -> Only executed when an action is called
- Column Selection -> Can select specific columns or create new ones
- Expression Support -> Supports complex column expressions and calculations

#### Common Use Cases
- Column Selection -> Choosing specific columns from a DataFrame
- Column Renaming -> Changing column names during selection
- Column Transformation -> Creating new columns with calculations
- Data Projection -> Reducing the number of columns in a DataFrame
- Schema Modification -> Changing the structure of the DataFrame

#### Sample Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SelectTransformation") \
    .getOrCreate()

# Create sample data
data = [
    ("John", 25, "New York", 50000),
    ("Alice", 30, "London", 75000),
    ("Bob", 17, "Paris", 30000),
    ("Emma", 22, "Tokyo", 45000),
    ("Mike", 35, "Berlin", 90000)
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "age", "city", "salary"])

# 1. Basic column selection
selected_df = df.select("name", "age")
print("Basic selection:")
selected_df.show()

# 2. Column selection with expressions
salary_in_k = df.select(
    col("name"),
    (col("salary") / 1000).alias("salary_in_k")
)
print("Salary in thousands:")
salary_in_k.show()

# 3. Multiple column expressions
complex_df = df.select(
    col("name"),
    col("age"),
    expr("CASE WHEN age >= 30 THEN 'Senior' ELSE 'Junior' END as experience_level"),
    (col("salary") * 1.1).alias("salary_with_bonus")
)
print("Complex selection:")
complex_df.show()

# 4. Using select with other transformations
filtered_selected = df.filter(col("age") >= 25) \
                     .select("name", "city", "salary")
print("Filtered and selected:")
filtered_selected.show()

# Stop Spark Session
spark.stop()
```

#### Best Practices
1. Select Early -> Use select early in the transformation chain to reduce data volume
2. Column Naming -> Use meaningful aliases for calculated columns
3. Expression Clarity -> Break complex expressions into multiple steps for better readability
4. Schema Awareness -> Be aware of the resulting schema after selection
5. Performance -> Select only the columns you need to improve performance

#### Performance Considerations
- Select is a narrow transformation, making it generally efficient
- Selecting fewer columns reduces memory usage and improves performance
- Complex expressions in select might impact performance
- Consider the order of select operations in the transformation chain
- Use appropriate column expressions for better optimization

### Map

The map transformation is a narrow transformation that applies a function to each element of an RDD, returning a new RDD with the transformed elements. It's one of the fundamental transformations in PySpark for data processing.

#### Key Characteristics of Map
- Narrow Transformation -> No data shuffling required
- One-to-One Transformation -> Each input element produces exactly one output element
- Lazy Evaluation -> Only executed when an action is called
- Preserves Partitioning -> Maintains the same number of partitions as the input RDD
- Type Flexibility -> Can transform data into different types

#### Common Use Cases
- Data Transformation -> Converting data from one format to another
- Feature Engineering -> Creating new features from existing data
- Data Cleaning -> Standardizing or normalizing data
- Type Conversion -> Converting data types
- Value Calculation -> Computing new values from existing data

#### Sample Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MapTransformation") \
    .getOrCreate()

# Create sample data
data = [
    ("John", 25, "New York", 50000),
    ("Alice", 30, "London", 75000),
    ("Bob", 17, "Paris", 30000),
    ("Emma", 22, "Tokyo", 45000),
    ("Mike", 35, "Berlin", 90000)
]

# Create RDD
rdd = spark.sparkContext.parallelize(data)

# 1. Basic mapping - convert salary to annual salary in thousands
salary_in_k = rdd.map(lambda x: (x[0], x[1], x[2], round(x[3]/1000, 2)))
print("Salary in thousands:")
print(salary_in_k.collect())

# 2. Complex mapping - create a dictionary with calculated values
employee_info = rdd.map(lambda x: {
    "name": x[0],
    "age": x[1],
    "city": x[2],
    "salary": x[3],
    "salary_category": "High" if x[3] > 60000 else "Medium" if x[3] > 40000 else "Low",
    "years_to_retirement": 65 - x[1]
})
print("Employee information with categories:")
print(employee_info.collect())

# 3. Type conversion and calculation
processed_data = rdd.map(lambda x: (
    x[0].upper(),  # Convert name to uppercase
    float(x[1]),   # Convert age to float
    x[2].lower(),  # Convert city to lowercase
    x[3] * 1.1     # Add 10% bonus to salary
))
print("Processed data with type conversions:")
print(processed_data.collect())

# 4. Mapping with multiple conditions
categorized_data = rdd.map(lambda x: {
    "name": x[0],
    "age_group": "Senior" if x[1] >= 30 else "Junior" if x[1] >= 18 else "Minor",
    "salary_level": "High" if x[3] >= 70000 else "Medium" if x[3] >= 40000 else "Low",
    "location_type": "Major City" if x[2] in ["New York", "London", "Tokyo"] else "Other"
})
print("Categorized employee data:")
print(categorized_data.collect())

# Stop Spark Session
spark.stop()
```

#### Best Practices
1. Function Simplicity -> Keep mapping functions simple and focused
2. Error Handling -> Include error handling in mapping functions
3. Type Consistency -> Ensure consistent output types
4. Memory Efficiency -> Avoid creating large objects in mapping functions
5. Documentation -> Document complex mapping functions

#### Performance Considerations
- Map is a narrow transformation, making it generally efficient
- Complex mapping functions can impact performance
- Consider the size of objects created in the mapping function
- Be mindful of memory usage when creating new objects
- Use appropriate data types to optimize memory usage

### FlatMap

The flatMap transformation is a narrow transformation that applies a function to each element of an RDD and flattens the results. Unlike map, which produces one output element for each input element, flatMap can produce zero, one, or multiple output elements for each input element.

#### Key Characteristics of FlatMap
- Narrow Transformation -> No data shuffling required
- One-to-Many Transformation -> Each input element can produce multiple output elements
- Lazy Evaluation -> Only executed when an action is called
- Flattening Operation -> Automatically flattens nested structures
- Type Flexibility -> Can transform data into different types

#### Common Use Cases
- Text Processing -> Splitting text into words or sentences
- Data Explosion -> Breaking down complex records into simpler ones
- Data Normalization -> Flattening nested data structures
- Feature Extraction -> Extracting multiple features from a single record
- Data Cleaning -> Splitting or expanding records based on conditions

#### Sample Implementation

```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FlatMapTransformation") \
    .getOrCreate()

# Create sample data
data = [
    ("John", ["Python", "Java", "Scala"]),
    ("Alice", ["Python", "R"]),
    ("Bob", ["Java"]),
    ("Emma", ["Python", "Java", "R", "Scala"]),
    ("Mike", ["Scala"])
]

# Create RDD
rdd = spark.sparkContext.parallelize(data)

# 1. Basic flatMap - split programming languages into individual records
languages = rdd.flatMap(lambda x: [(x[0], lang) for lang in x[1]])
print("Programming languages per person:")
print(languages.collect())

# 2. Complex flatMap - create multiple records with different attributes
detailed_records = rdd.flatMap(lambda x: [
    (x[0], lang, "Primary" if i == 0 else "Secondary")
    for i, lang in enumerate(x[1])
])
print("Detailed language records:")
print(detailed_records.collect())

# 3. Text processing example
text_data = [
    "Hello World",
    "PySpark is awesome",
    "Data Engineering",
    "Big Data Processing"
]
text_rdd = spark.sparkContext.parallelize(text_data)

# Split text into words and create word count pairs
word_pairs = text_rdd.flatMap(lambda x: [(word, 1) for word in x.lower().split()])
print("Word pairs:")
print(word_pairs.collect())

# 4. Data normalization example
nested_data = [
    ("Project A", ["Task 1", "Task 2", "Task 3"]),
    ("Project B", ["Task 4"]),
    ("Project C", ["Task 5", "Task 6"])
]
project_rdd = spark.sparkContext.parallelize(nested_data)

# Flatten project-task relationships
project_tasks = project_rdd.flatMap(lambda x: [(x[0], task) for task in x[1]])
print("Project-Task relationships:")
print(project_tasks.collect())

# Stop Spark Session
spark.stop()
```

#### Best Practices
1. Function Clarity -> Keep flatMap functions clear and well-documented
2. Output Control -> Be mindful of the number of output elements generated
3. Memory Management -> Consider memory usage when generating multiple outputs
4. Error Handling -> Include error handling in flatMap functions
5. Data Validation -> Validate input data before processing

#### Performance Considerations
- FlatMap is a narrow transformation, making it generally efficient
- Be cautious of data explosion (generating too many output elements)
- Consider the memory impact of flattening operations
- Monitor the size of output RDDs
- Use appropriate data structures for output elements

### Display

The display functionality in PySpark is a powerful tool for visualizing and presenting data in interactive environments like Databricks notebooks. It provides various ways to view and analyze data in a user-friendly format.

#### Key Characteristics of Display
- Interactive Visualization -> Supports multiple visualization types
- Data Exploration -> Helps in quick data analysis
- Format Flexibility -> Can display data in different formats
- Real-time Updates -> Reflects changes in data immediately
- Customization Options -> Allows customization of display parameters

#### Common Use Cases
- Data Preview -> Quick view of DataFrame contents
- Data Visualization -> Creating charts and graphs
- Data Analysis -> Exploring data distributions
- Result Presentation -> Displaying query results
- Debugging -> Inspecting intermediate results

#### Sample Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DisplayExamples") \
    .getOrCreate()

# Create sample data
data = [
    ("John", "Sales", 50000, "New York"),
    ("Alice", "Marketing", 75000, "London"),
    ("Bob", "Sales", 30000, "Paris"),
    ("Emma", "Engineering", 45000, "Tokyo"),
    ("Mike", "Engineering", 90000, "Berlin"),
    ("Sarah", "Marketing", 65000, "London"),
    ("Tom", "Sales", 55000, "New York")
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "department", "salary", "city"])

# 1. Basic display
print("Basic DataFrame display:")
df.show()

# If we are using Databricks Notebooks, we can use 
display(df)


# 2. Display with truncation control
print("\nDisplay with full content:")
df.show(truncate=False)

# 3. Display with specific number of rows
print("\nDisplay first 3 rows:")
df.show(3)

# 4. Display with vertical format
print("\nDisplay in vertical format:")
df.show(2, vertical=True)

# 5. Display with custom column selection
print("\nDisplay specific columns:")
df.select("name", "department").show()

# 6. Display with aggregation
print("\nDisplay department statistics:")
dept_stats = df.groupBy("department") \
    .agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        sum("salary").alias("total_salary")
    )
dept_stats.show()

# 7. Display with sorting
print("\nDisplay sorted by salary:")
df.orderBy(col("salary").desc()).show()

# 8. Display with filtering
print("\nDisplay high-salary employees:")
df.filter(col("salary") > 60000).show()

# Stop Spark Session
spark.stop()
```

#### Best Practices
1. Truncation Control -> Use truncate parameter appropriately
2. Row Limiting -> Limit the number of rows displayed for large datasets
3. Column Selection -> Select only necessary columns for display
4. Format Selection -> Choose appropriate display format for the data
5. Performance -> Be mindful of display operations on large datasets

#### Performance Considerations
- Display operations can be memory-intensive for large datasets
- Consider using limit() before display for large DataFrames
- Be cautious with display operations in production code
- Use appropriate display formats for different data types
- Consider the impact on notebook performance

