# Hadoop Tutorial - Efficient data ingestion

The Hadoop ecosystem is the leading opensource platform for distributed storage and processing of "big data". The Hadoop platform is available at CERN as a central service provided by the IT department.

Real-time data ingestion to Hadoop ecosystem due to the system specificity is non-trivial process and requires some efforts (which is often underestimated) in order to make it efficient (low latency, optimize data placement, footprint on the cluster).

In this tutorial attendees will learn about:

* The important aspects of storing the data in Hadoop Distributed File System (HDFS). 
* Data ingestion techniques and engines that are capable of shipping data to Hadoop in an efficient way.
* Setting up a full data ingestion flow into a Hadoop Distributed Files System from various sources (streaming, log files, databases) using the best practices and components available around the ecosystem (including Sqoop, Flume, Kafka, Gobblin).

A hands-on video has been recorded where we go through all the commands explained during the presentation: https://www.youtube.com/watch?v=dYV7Ay0ufQk