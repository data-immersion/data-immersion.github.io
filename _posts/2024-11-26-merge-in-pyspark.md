---
title: "Merge in Pyspark"
date: 2024-11-26 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python, Merge, Upsert
---

# Merge in Pyspark

Merge operations (also known as upserts) are essential operations in data processing that allow you to combine data from multiple sources while handling updates, inserts, and deletes. PySpark provides powerful merge capabilities that are crucial for data integration and synchronization.

## Understanding Merge Operations

### What is a Merge?
A merge operation combines data from two DataFrames based on matching keys, allowing you to:
1. Update existing records
2. Insert new records
3. Delete records
4. Handle conflicts between source and target data

### Basic Merge Syntax
```python
# Basic merge syntax
df1.merge(df2, on="key_column", how="merge_type")
```

## Types of Merge Operations


### 1. Delta Lake Merge Operations
Delta Lake provides a more sophisticated merge operation with better performance and ACID guarantees:

```python
from delta.tables import DeltaTable

# Create or load Delta table
delta_table = DeltaTable.forName(spark, "products")

# Perform merge operation
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    condition="target.price != source.price",
    set={"price": "source.price"}
).whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "price": "source.price"
    }
).execute()
```

### 2. MergeBuilder Scenarios

The MergeBuilder in Delta Lake provides a flexible and powerful way to perform merge operations. Here are different scenarios using MergeBuilder:

#### Basic MergeBuilder
```python
from delta.tables import DeltaTable

# Create or load Delta table
delta_table = DeltaTable.forName(spark, "products")

# Basic MergeBuilder
merge_builder = delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
)

# Execute merge
merge_builder = merge_builder.whenMatchedUpdate(
    set={"price": "source.price"}
)
merge_builder = merge_builder.whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "price": "source.price"
    }
)
merge_builder.execute()
```

#### MergeBuilder with Multiple Conditions
```python
# MergeBuilder with multiple conditions
merge_builder = delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
)

# Multiple whenMatchedUpdate clauses
merge_builder = merge_builder.whenMatchedUpdate(
    condition="target.price < source.price",
    set={"price": "source.price", "updated_at": "current_timestamp()"}
)
merge_builder = merge_builder.whenMatchedUpdate(
    condition="target.price > source.price",
    set={"price": "target.price * 0.9", "updated_at": "current_timestamp()"}
)
merge_builder = merge_builder.whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "price": "source.price",
        "created_at": "current_timestamp()"
    }
)
merge_builder.execute()
```

#### MergeBuilder with Delete Operations
```python
# MergeBuilder with delete operations
merge_builder = delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
)

merge_builder = merge_builder.whenMatchedDelete(
    condition="source.status = 'deleted'"
)
merge_builder = merge_builder.whenMatchedUpdate(
    condition="source.status != 'deleted'",
    set={
        "price": "source.price",
        "status": "source.status",
        "updated_at": "current_timestamp()"
    }
)
merge_builder = merge_builder.whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "price": "source.price",
        "status": "source.status",
        "created_at": "current_timestamp()"
    }
)
merge_builder.execute()
```

#### MergeBuilder with Conditional Operations
```python
# MergeBuilder with conditional operations
merge_builder = delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
)

# Conditional updates based on business rules
merge_builder = merge_builder.whenMatchedUpdate(
    condition="""
        CASE 
            WHEN source.status = 'active' AND target.status = 'inactive' THEN true
            WHEN source.price > target.price * 1.1 THEN true
            WHEN source.last_updated > target.last_updated THEN true
            ELSE false
        END
    """,
    set={
        "price": """
            CASE 
                WHEN source.status = 'active' THEN source.price
                WHEN source.price > target.price * 1.1 THEN target.price * 1.05
                ELSE target.price
            END
        """,
        "status": "source.status",
        "last_updated": "source.last_updated",
        "updated_at": "current_timestamp()"
    }
)
merge_builder = merge_builder.whenMatchedDelete(
    condition="source.status = 'deleted' AND target.last_updated < date_sub(current_date(), 30)"
)
merge_builder = merge_builder.whenNotMatchedInsert(
    condition="source.status != 'deleted'",
    values={
        "id": "source.id",
        "name": "source.name",
        "price": "source.price",
        "status": "source.status",
        "last_updated": "source.last_updated",
        "created_at": "current_timestamp()"
    }
).execute()
```

#### MergeBuilder with Dynamic Conditions
```python
# MergeBuilder with dynamic conditions
def create_merge_builder(delta_table, source_df, business_rules):
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"),
        "target.id = source.id"
    )
    
    # Build dynamic conditions based on business rules
    update_condition = " OR ".join([
        f"source.{rule['field']} {rule['operator']} target.{rule['field']}"
        for rule in business_rules['update_rules']
    ])
    
    # Build dynamic set statements
    set_statements = {
        field: f"source.{field}"
        for field in business_rules['update_fields']
    }
    set_statements['updated_at'] = 'current_timestamp()'
    
    # Apply conditions
    merge_builder = merge_builder.whenMatchedUpdate(
        condition=update_condition,
        set=set_statements
    )
    
    # Add insert conditions if specified
    if 'insert_condition' in business_rules:
        merge_builder = merge_builder.whenNotMatchedInsert(
            condition=business_rules['insert_condition'],
            values=business_rules['insert_values']
        )
    
    return merge_builder

# Example usage
business_rules = {
    'update_rules': [
        {'field': 'price', 'operator': '>'},
        {'field': 'status', 'operator': '!='},
        {'field': 'last_updated', 'operator': '>'}
    ],
    'update_fields': ['price', 'status', 'last_updated'],
    'insert_condition': "source.status != 'deleted'",
    'insert_values': {
        'id': 'source.id',
        'name': 'source.name',
        'price': 'source.price',
        'status': 'source.status',
        'last_updated': 'source.last_updated',
        'created_at': 'current_timestamp()'
    }
}

# Create and execute merge
merge_builder = create_merge_builder(delta_table, source_df, business_rules)
merge_builder.execute()
```

#### MergeBuilder with Conditional Aggregations
```python
# MergeBuilder with conditional aggregations
merge_builder = delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
)

merge_builder = merge_builder.whenMatchedUpdate(
    condition="target.price != source.price",
    set={
        "price": "source.price",
        "price_history": """
            CASE 
                WHEN target.price_history IS NULL THEN array(source.price)
                ELSE array_union(target.price_history, array(source.price))
            END
        """,
        "avg_price": """
            CASE 
                WHEN target.price_history IS NULL THEN source.price
                ELSE (array_sum(target.price_history) + source.price) / 
                     (size(target.price_history) + 1)
            END
        """,
        "max_price": """
            CASE 
                WHEN target.price_history IS NULL THEN source.price
                ELSE greatest(array_max(target.price_history), source.price)
            END
        """,
        "updated_at": "current_timestamp()"
    }
)
merge_builder = merge_builder.whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "price": "source.price",
        "price_history": "array(source.price)",
        "avg_price": "source.price",
        "max_price": "source.price",
        "created_at": "current_timestamp()"
    }
)
merge_builder.execute()
```

### 3. Complex Merge Scenarios

#### Handling Multiple Conditions
```python
# Sample data with multiple conditions
source_data = [
    (1, "Product A", 100, "Active"),
    (2, "Product B", 200, "Inactive"),
    (3, "Product C", 300, "Active")
]

target_data = [
    (1, "Product A", 150, "Inactive"),
    (2, "Product B", 200, "Active"),
    (4, "Product D", 400, "Active")
]

# Create DataFrames
source_df = spark.createDataFrame(source_data, ["id", "name", "price", "status"])
target_df = spark.createDataFrame(target_data, ["id", "name", "price", "status"])

# Complex merge with multiple conditions
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    condition="target.price != source.price OR target.status != source.status",
    set={
        "price": "source.price",
        "status": "source.status"
    }
).whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "price": "source.price",
        "status": "source.status"
    }
).execute()
```

## Best Practices for Merge Operations

### 1. Performance Optimization
- Use appropriate partition columns
- Consider using `repartition()` before merge operations
- Cache frequently used DataFrames
- Use Delta Lake for better performance and ACID guarantees

### 2. Data Quality
- Handle null values appropriately
- Validate data before merge
- Use appropriate merge conditions
- Consider data type compatibility

### 3. Error Handling
```python
try:
    # Perform merge operation
    delta_table.alias("target").merge(
        source_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(
        set={"price": "source.price"}
    ).whenNotMatchedInsert(
        values={
            "id": "source.id",
            "name": "source.name",
            "price": "source.price"
        }
    ).execute()
except Exception as e:
    print(f"Merge operation failed: {str(e)}")
    # Handle error appropriately
```

## Common Use Cases

### 1. Incremental Updates
```python
# Sample incremental update
def perform_incremental_update(source_df, target_table):
    delta_table = DeltaTable.forName(spark, target_table)
    
    delta_table.alias("target").merge(
        source_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(
        condition="target.last_updated < source.last_updated",
        set={
            "price": "source.price",
            "last_updated": "source.last_updated"
        }
    ).whenNotMatchedInsert(
        values={
            "id": "source.id",
            "name": "source.name",
            "price": "source.price",
            "last_updated": "source.last_updated"
        }
    ).execute()
```

### 2. Data Deduplication
```python
# Sample deduplication merge
def deduplicate_data(source_df, target_table):
    delta_table = DeltaTable.forName(spark, target_table)
    
    # Remove duplicates from source
    deduplicated_source = source_df.dropDuplicates(["id"])
    
    delta_table.alias("target").merge(
        deduplicated_source.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(
        set={"price": "source.price"}
    ).whenNotMatchedInsert(
        values={
            "id": "source.id",
            "name": "source.name",
            "price": "source.price"
        }
    ).execute()
```

### 3. Slowly Changing Dimensions (SCD)
```python
# Sample SCD Type 2 implementation
def scd_type2_merge(source_df, target_table):
    delta_table = DeltaTable.forName(spark, target_table)
    
    delta_table.alias("target").merge(
        source_df.alias("source"),
        "target.id = source.id AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.price != source.price",
        set={
            "is_current": "false",
            "end_date": "current_date()"
        }
    ).whenNotMatchedInsert(
        values={
            "id": "source.id",
            "name": "source.name",
            "price": "source.price",
            "start_date": "current_date()",
            "end_date": "null",
            "is_current": "true"
        }
    ).execute()
```

## Performance Considerations

### 1. Partitioning Strategy
```python
# Example of partitioning before merge
def optimized_merge(source_df, target_table):
    # Repartition source data
    partitioned_source = source_df.repartition("id")
    
    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias("target").merge(
        partitioned_source.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(
        set={"price": "source.price"}
    ).whenNotMatchedInsert(
        values={
            "id": "source.id",
            "name": "source.name",
            "price": "source.price"
        }
    ).execute()
```

### 2. Caching Strategy
```python
# Example of caching for frequent merges
def cached_merge(source_df, target_table):
    # Cache source data if it will be used multiple times
    source_df.cache()
    
    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias("target").merge(
        source_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(
        set={"price": "source.price"}
    ).whenNotMatchedInsert(
        values={
            "id": "source.id",
            "name": "source.name",
            "price": "source.price"
        }
    ).execute()
    
    # Unpersist when done
    source_df.unpersist()
```

## Conclusion

Merge operations in PySpark are powerful tools for data integration and synchronization. Understanding the different types of merge operations and their use cases is crucial for effective data processing. Remember to:

- Choose the right merge strategy for your use case
- Optimize performance with proper partitioning and caching
- Handle errors and edge cases appropriately
- Follow best practices for data quality and consistency

Key takeaways:
- Use Delta Lake for better performance and ACID guarantees
- Implement appropriate error handling
- Consider performance implications
- Follow best practices for data quality