# Cloud_Computing_Assignment

This project is part of the Cloud Computing Assignment for Cranfield University MSc. CSTE 2019-2020.

The assignment is inspired by DEBS 2015 challenge : https://debs.org/grand-challenges/2015/
It aims to solve a restricted part of the challenge using Spark. The assignment can be found at `doc/assignment-2019-2020.pdf`.
 
Query 1: Frequent Routes
The goal of the query is to find the top 10 most frequent routes during the last 30 minutes. 
A route is represented by a starting grid cell and an ending grid cell.
 
 ## Setting up spark
 
 Following information is for installing spark on a EC2 RHEL-8.0.0 instance
 
 -> Install java
```bash
sudo yum update
yum install java-1.8.0-openjdk
``` 
-> Download apache spark binaries (This application as been developed for spark-3.0.0-preview2-bin-hadoop2.7.tgz ): 
 https://www.apache.org/dyn/closer.lua/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
 ```
yum install wget
mkdir spark
cd spark
wget <link to Apache Spark Distribution>
tar xvzf spark-3.0.0-preview2-bin-hadoop2.7.tgz
```
-> export  `$JAVA_HOME`

```export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el8_1.x86_64/jre/```

 (for setting up a complete cluster on aws cf : https://www.novixys.com/blog/setup-apache-hadoop-cluster-aws-ec2/)
  
 ## Get this application 
 
Clone this repository :
  
 ```
 wget https://github.com/Inao0/Cloud_Computing_Assignment/archive/master.zip
 sudo yum install unzip
 unzip master.zip
```

Download sbt : (cf https://www.scala-sbt.org/download.html)

```
curl https://bintray.com/sbt/rpm/rpm > bintray-sbt-rpm.repo
sudo mv bintray-sbt-rpm.repo /etc/yum.repos.d/
sudo yum install sbt
```

Packaging the application : 
`sbt package`

## Get Data

Some fake data are provided in `data/test_data` with a corresponding `data/test_data_README.md` for testing purposes

Actual  data can be download from the DEBS 2015 challenge website : https://debs.org/grand-challenges/2015/. 
Two datasets are provided. One contain the first twenty days of 2013 and the other the entire year.


## Run the application 

In order to be use some of spark functionality we need to export the following environment variable :     
`export SPARK_TESTING=$true`

Cf `CloudComputingAssignment/Query1_Stateful_Streaming.scala:31` for more explanations. 

In order to run the spark application that we just packaged, assuming that the zip file has been unzipped in the home
folder, we use:
 ```
 ./../spark/spark-3.0.0-preview2-bin-hadoop2.7/bin/spark-submit --class "CloudComputingAssignment.Query1_Stateful_Streaming" --master local[*] target/scala-2.12/cloud_computing_assignment_2.12-0.1.jar
```
There are three implementation that can be run Query1_Simple_Dataset, Query1_Batch and Query1_Stateful_Streaming

The input data files should be placed in `data/taxiRides`.
The output files will be found in `Result` folder. 
For simple_dataset and streaming implementations the results is 
outputted across different files.A small makefile is available to help merge and sort these output data.

- `make sortStreaming` can be use to merge and sort files outputted by Query1_Stateful_Streaming. 
The resulting file will be ordered by time_window and then last update. In case of multiple rows corresponding to the
top N for a given time window, only the most recent one should be taken into account

- `make sortDataset` can be use to merge and sort files outputted by Query1_Simple_Dataset.
The resulting file will be ordered by time_window. In this implementation each element of the top N element
generate a row. Unfortunately the rank column being the last printed, the sort command line will not order elements
properly for a same time Window. 

- `make sortBatch` can be used to sort the output file by time window if several batches have been computed. In this 
implementation each element of the top N element also generate a row. Unfortunately the rank column being the last 
printed, the sort command line will not order elements properly for a same time Window. Moreover if several batches
arrive it wont be possible to distinguish from which batch the element is coming from.

- `make cleanResults` remove the Result folder. Query1_Simple_Dataset require that the folder to which it is supposed to
output does not exist before running the application.   
   
