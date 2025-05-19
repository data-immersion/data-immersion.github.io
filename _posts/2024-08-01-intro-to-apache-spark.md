---
title: "Introduction to Apache Spark"
date: 2024-08-01 20:14 +0300
categories: [Data Engineering, Apache Spark, Databricks]
tags: Data Engineering, Apache Spark, Databricks
---

# Introduction to Apache Spark

### What is Apache Spark?

- Apache Spark is an open-source unified analytics engine for large-scale data processing. Spark provides an interface for programming clusters with implicit data parallelism and fault tolerance. 
- Apache Spark has its architectural foundation in the Resilient Distributed Dataset (RDD), a read-only multiset of data items distributed over a cluster of machines, that is maintained in a fault-tolerant way. 
- Dataframe API was released as an abstraction on top of the RDD, followed by Dataset API.
- RDD was the primary application programming interface (API), in Spark 1.x. but as of Spark 2.x use of the Dataset API is encouraged even though the RDD API is not deprecated, the RDD technology still underlies the Dataset API.

### Development of Apache Spark

        Apache Spark was initially developed by researchers at the AMPLab at UC Berkeley in 2009, and open sourced in 2010 under a BSD license, including Matei Zaharia, who is often credited as the lead developer of the project. This project was donated to the Apache Software Foundation in 2013 and switched its license to Apache 2.0. 

        Spark founder Matei Zaharia's company Databricks set a new world record in large scale sorting using Spark in 2014, by the end of 2015 it hasd 1000's of contibutors making it one of the most active projects in the Apache Software Foundation and one of the most active open source big data projects.

### Why Apache Spark was developed?

- To overcome the limitations of **MapReduce**, Spark and its RDD's were delevoped.
- MapReduce is a  cluster computing paradigm, which forces a particular linear dataflow structure on distributed programs.
- MapReduce programs read input data from disk, map a function across the data, reduce the results of the map, and store reduction results on disk. 
- On the other hand, Spark's RDDs function as a working set for distributed programs that offers a restricted form of distributed shared memory.

### Working of Spark

- Apache Spark requires a cluster manager and a distributed storage system.
- Spark supports some of these clusters for cluster management
    - Hadoop YARN
    - Apache Mesos
    - Native spark cluster
    - Kubernetes

### Working steps of Spark

1) Submitting an Application:
    - A Spark application is written using high-level APIs in languages like Scala, Python, Java, or R.
    - The application includes the driver program, which submits the application to the cluster manager

2) Creating RDDs:
    - It stands for Resilient Distributed Dataset.
    - RDDs are created from data sources (e.g., HDFS, local file systems, S3, HBase) or by transforming existing RDDs.
    - Transformations are lazily evaluated, meaning they are not executed until an action is called

3) Transformations and Actions:
    - Transformations are the operations on RDDs that produce a new RDD (e.g., map, filter). These are lazy operations, and define the logical execution plan.
    - Actions are the operations that trigger the execution of the transformations (e.g., reduce, collect). These produce a result or write data to storage.

4) Building the DAG
    - It stands for Directed Acyclic Graph.
    - When an action is called, Spark constructs a DAG of stages and tasks based on the RDD transformations.
    - Each stage in the DAG is a set of transformations that can be executed together before shuffling data between nodes.

5) Task Scheduling and Execution:
    - The driver program sends tasks to the cluster manager, which allocates resources and schedules tasks on worker nodes.
    - Executors on the worker nodes execute the tasks, performing the necessary computations on partitions of the RDD.

6) In-Memory Processing:
    - Spark keeps intermediate data in memory to speed up processing. This avoids the overhead of writing to and reading from disk
    - In-memory caching can be explicitly controlled using the persist or cache methods on RDDs.

7) Fault Tolerance:
    - Spark provides fault tolerance through RDD lineage. If a partition of an RDD is lost, Spark can recompute it using the original transformations
    - Checkpoints can also be used to save the state of RDDs to reliable storage, reducing recomputation overhead.
