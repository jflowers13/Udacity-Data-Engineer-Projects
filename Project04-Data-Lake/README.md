# Project 4: Data Lake using Apache Spark

### Introduction

The purpose of this project is to create a Data Lake that enables the Sparkify analytics team to gain key insights into the activities of Sparkify users.  User logs and song data are stored in JSON format in an S3 bucket and loaded into Fact/Dimension tables in Spark using an ETL process.  After the data is processed with Spark, it is loaded into another S3 bucket in Parquet format.

### Files in project

**README.md** - The current file that is being read.  Provides a project overview and gives information on the project.

**dl.cfg** - This file contains the AWS credentials for the project, including the Access Key and Secret Key.  **Important:  Remove sensitive information before sharing**

**etl.py** - Contains the python code for reading the song and user log data from S3, processing the data, and then writing the processed data back to S3.

**sparkifydev.ipynb** - Notebook used to test code with small data set for implementation of the ETL pipeline in **etl.py**.  Meant to be run locally.

### Schema

#### Fact Table: songplays
| Field       	| Type    	| Key 	| Description   	|
|-------------	|---------	|-----	|---------------	|
| songplay_id 	| SERIAL  	| PK  	|               	|
| start_time  	| VARCHAR 	| FK  	|               	|
| user_id     	| INTEGER 	| FK  	|               	|
| level       	| VARCHAR 	|     	|               	|
| song_id     	| VARCHAR 	| FK  	|               	|
| artist_id   	| VARCHAR 	| FK  	|               	|
| session_id  	| INTEGER 	|     	|               	|
| location    	| VARCHAR 	|     	| User location 	|
| user_agent  	| VARCHAR 	|     	|               	|

#### Dimension Table: songs
| Field     	| Type    	| Key 	| Description 	|
|-----------	|---------	|-----	|-------------	|
| song_id   	| VARCHAR 	| PK  	|             	|
| title     	| VARCHAR 	|     	|             	|
| artist_id 	| VARCHAR 	|     	|             	|
| year      	| INTEGER 	|     	|             	|
| duration  	| NUMERIC 	|     	|             	|

#### Dimension Table: artists
| Field     	| Type    	| Key 	| Description     	|
|-----------	|---------	|-----	|-----------------	|
| artist_id 	| VARCHAR 	| PK  	|                 	|
| name      	| VARCHAR 	|     	| Artist Name     	|
| location  	| VARCHAR 	|     	| Artist Location 	|
| latitude  	| NUMERIC 	|     	|                 	|
| longitude 	| NUMERIC 	|     	|                 	|

#### Dimension Table: users
| Field      	| Type    	| Key 	| Description        	|
|------------	|---------	|-----	|--------------------	|
| user_id    	| INTEGER 	| PK  	|                    	|
| first_name 	| VARCHAR 	|     	|                    	|
| last_name  	| VARCHAR 	|     	|                    	|
| gender     	| VARCHAR 	|     	|                    	|
| level      	| VARCHAR 	|     	| Subscription Level 	|

#### Dimension Table: time
| Field      	| Type    	| Key 	| Description 	|
|------------	|---------	|-----	|-------------	|
| start_time 	| VARCHAR 	| PK  	|             	|
| hour       	| INTEGER 	|     	|             	|
| day        	| INTEGER 	|     	|             	|
| week       	| INTEGER 	|     	|             	|
| month      	| INTEGER 	|     	|             	|
| year       	| INTEGER 	|     	|             	|
| weekday    	| VARCHAR 	|     	|             	|

### ETL Process

The ETL process pulls data from the S3 Data Bucket at `s3a://udacity-dend/` in folders `song_data` and `log_data`.  All the data is stored in JSON format.  The reading of the JSON files is configured to handle multiline data and also to create a separate column for corrupted data while letting the intact data go to its destination.

The data for generating the `songs` and `artists` tables is ingested from the `song_data` folder with a specified schema.  The `songs` table is generated first using the column names from the source and the `artists` table is generated second with the column names changed on a few of the columns.  In preparation for creating the `songplays` table in a later step, a temporary view named `song_df_table` is created.

The data for generating the `users` and `time` tables is ingested from the `log_data` folder using an inferred schema.  To create the users table, user data is selected from the data frame containing the event data and the column names are changed to conform to the destination table.  The `time` table is created by first converting the timestamp from milliseconds to seconds using a udf function and then assigning it to the log data frame as the `start_time` column.  Using the `start_time` column, all the other time-related columns are created.

The `songplays` Fact table is created by combining the log data with song data using a join on artist name, song title and song length using a left outer join from the song data view (created in an earlier step as `song_df_table`) to the data frame containing the log data.  All data from the log data frame and the song data that matches it goes into the `songplays` table.

### Instructions

To run the etl.py file, do the following:

1.  Create an IAM role with administrative rights, get the key pair for it in a .pem file and save a copy of the key pair .csv file in a safe place.

2.  Save the key pair .pem file in a safe, easily accessed place, and if you are using Linux, chmod it to 400 as it only needs to be read by ssh and scp

3.  Put the access key and secret key in the dl.cfg file

4.  Create a cluster in AWS EMR using Spark, m4.xlarge is a sufficient hardware instance type with 3 instances for running the project.  Additionally, choose the key pair you intend to use in the EC2 Key Pair drop down.

5.  After bringing up the Spark cluster and getting a `waiting` status, create an ssh tunnel to the Spark cluster using the key pair, set proxy on your browser to the emr-socks-proxy to bring up the application user interfaces for monitoring the Spark job.

6.  scp the dl.cfg and etl.py files to the `/home/hadoop` folder on the cluster.

7.  After copying the files above to the cluster, ssh into the cluster and run `submit-spark etl.py`.  It takes close to an hour for the job to run until finished.

8.  After the job is finished running and all the tables are written to the destination S3 bucket, close the connections and tunnel to the Spark cluster, and terminate the cluster.
