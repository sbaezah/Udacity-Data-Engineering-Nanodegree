# Udacity Data Engineer NanoDegree
# Project 4: Apache Spark & Data Lake
# Sebastian Baeza - September 2019

## Summary
* [Intro](#Intro)
* [ETL Datalake Spark process](#ETL-Data-Lake-Spark-process)
* [Project structure](#Project-structure)
--------------------------------------------

#### Intro
In this project, the data source is provided by Udacity in S3 bucket. This bucket contains two folders, one with info about the songs and other with the log files, all in JSON format. 
All the process will be work with Spark under pySpark.

--------------------------------------------
#### ETL Datalake Spark process

This ETL executes four functions:

<b> create_spark_session: </b> This function creates the spark object that will be used in the ETL process.

<b> process_song_data: </b> This function read the JSON files about songs contained in the S3 bucket. Next load the tables "songs" and "artists".

<b> process_log_data: </b> This function read the JSON files about logs contained in the S3 bucket. First, filter the data by the action called "NextSong". Next load the tables "users", "time" and the "songplays" table. The "songplays" table requires a little bit more work and coding to create because it depends on the join between two dataframes.

<b> process_report_data: </b> This function read the tables loaded in spark across the parquet files and show the row count for each table created.

-------------------------
#### Project structure
This is the project structure, if the bullet contains:

* <b> /data </b> - This folder contains 2 zip files for work with a small dataset because the original dataset takes more time (a lot of time) if you want to test your code. Remember to unzip the files if you wish to work with them.
* <b> etl.py </b> - This script runs all the ETL process.
* <b> dl.cfg </b> - Is a config file where the user have to insert the AWS credentials.