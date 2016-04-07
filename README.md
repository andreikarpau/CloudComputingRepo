# CloudComputingRepo
Repository for cloud computing projects and study files.
Contains mainly cloud solutions on **Java**. Some projects use **C++**.

## AviationPrj
This is a capstone project for making different computations on **Big Dataset** (ca 200 000 000 records) using **Hadoop** and **Kafka** + **Spark Streaming** frameworks, as well as **Cassandra** NoSQL Database. Solutions are mainly made on **Java**. The data processing is done with **R**.

## CourseraCloudApplicationsAssignments
The solutions for programming assignments for Coursera Cloud Applications course. It contains the **MapReduce**, **Apache Storm**, **HBase**, **Giraph** and **Spark MlLib** solutions. The solutions are mainly made on **Java**.

## CloudSensorsFlowPrj
Creating a concept of system for dealing with multiple sources of sensors data. The project is made on **Java**. It contains a Udp Values Generator which emulates sensors on client side and sends the data to cloud. The data flow is being processed by **Apachi NiFi** cluster and then send to **Hadoop Ecosystem** cluster for furher reworking. The following systems are used on Hadoop side: **Spark Streaming**, **Spark**, **HDFS**, **HBase**, **Kafka**.

## MembershipGossipStyle
The implementation of Gossip Style membership protocol for P2P connections. It implements the gossip style hartbeating that is used in some real cloud systems. Project is implemented on **C++**. Protocol satisfies following: *Completeness all the time* (every non-faulty process, must detect every node join, failure, and leave), *accuracy of failure detection*.

## KeyValueStore
The implementation of Fault-Tolerant Key-Value store that have ring based implementation (similar to Cassandra). The store provides: *CRUD Operations*, *Load-Balancing*, *Fault-Tolerance* (via replication), *Quorum consistency level* and *Stabilization* after failure functionality. The implementaion is made on **C++**.
