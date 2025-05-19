---
title: "Narrow vs Wide Transformations"
date: 2024-10-20 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks, PySpark, Python]
tags: Data Engineering, Apache Spark, Databricks, PySpark, Python
---

# Narrow vs Wide Transformations

### What is Narrow Transformations?
- Narrow transformations are operations where each input partition contributes to only one output partition.
- This means that the transf`ormation does not require data to be shuffled across the network.
- As a result, narrow transformations are more efficient because they allow data to be processed locally, without the overhead of data movement.