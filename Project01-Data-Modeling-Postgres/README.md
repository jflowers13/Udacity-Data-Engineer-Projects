# Overview

The purpose of this project is to provide a database to store app usage data for the Sparkify Analytics team.  The log files and song data are stored on the local file system in JSON format and loaded into the datase using ETL pipelines.  With the data loaded into the database, the analytics team is able to write SQL queries and gain more insight into what songs Sparkify users are listening to.



# Project Files

+ create_tables.py - This file is run to drop tables in the sparkify database schema if present and create a fresh schema for the sparkify database.

+ test.ipynb - Jupyter notebook with queries that checks the first few rows of each table for testing purposes.

+ etl.ipynb - Jupyter notebook that contains detailed instructions on the ETL process for each of the tables.  This notebook reads and processes a single file from song_data and log_data into each of the database tables.

+ etl.py - Reads and processes all files from the song_data ands log_data folders and loads it into the database.

+ sql_queries.py - Contains queries for dropping tables, creating tables and inserting data into tables.

+ README.md - Markdown file used for documenting the project



# Database Schema for Sparkify

The sparkify database uses a star schema where the fact table is named songplays and the dimension tables are named users, songs, artists and time.

1. songs - This dimension table stores song information

2. artists - Stores information on artists

3. time - Stores song play time of when Sparkify users play songs

4. users - This is the dimension table that stores information on the users of the Sparkify app

5. songplays - This is the fact table and is used to tie together the user log data and song data


# ETL Process Overview

How the ETL process works is the song files and log files are read from the local file system into Pandas data frames, undergo transformations and then are loaded into the database with insert statements.


## ETL Process for songs table

To populate the songs table, the song data is read into a Pandas data frame. Then song id, song title, artist id, year and duration are selected and placed in a list data structure before loading into the songs tabe with an insert query stored in a variable.  The insert query does nothing on conflict to avoid loading a record that is already in the table.


## ETL Process for artists table

The artists table is populated in the same way as the songs table.  After reading the song data files into a Pandas data frame, the artist id, artist name, artist location, artist latitude and artist longitude are selected and placed in a list data structure and then loaded into the table with insert queries.  The insert query for the artists table does nothing on conflict to avoid loading duplicate records.


## ETL Process for time table

The time table is populated using data from the user logs.  Using the log data that is parsed into a Pandas data frame from the logs on the local file system, the records get paired down to only records with the "NextSong" action.  The timestamps in the log files are stored in milliseconds and is converted into a Pandas timestamp in the data frame.  From the new timestamp, timestamp in seconds, hour, day, week of year and weekday are extracted and placed in a time data list.  The time data list is then used to create a data frame, along with column headers.  This creates the time data frame, which is then loaded into the time table using an insert query.  On conflict, the insert statement does nothing in order to avoid loading duplicate records.


## ETL Process for user table

The user data is extracted from the log file data frame.  Columns that are selected include user id, first name, last name, gender and subscription level before getting loaded into a user data frame.  The records are then loaded one at a time into the users table using an insert query.  When there is a conflict on the subscription level, an update occurs as user subscription level can change over time.


## ETL Process for sonplays table

Populating the songplays table involves log data and song data.  Both song id and artist id are used to tie both data sources together for for loading the relevant data into the songplays table and are pulled from the songs table.  After getting the artist ids and song ids, timestamp, user id, subscription level, song id, artist id, session id, location and user agent are added to a list data structure, which then are all loaded into the songplays table with an insert query.



# Instructions

To run the project, the schema first needs to be created before running the ETL pipeline.  Once the schema is created and the data is loaded through the ETL pipeline, there is a test script in a Jupyter notebook that can be used to test the database.  The instructions are as follows:


1. Open a terminal and run the following command to create the schema:

        python create_tables.py

2. To load the data in the new schema, run the following command:

        python etl.py

3. For testing, open test.ipynb and execute each statement one at a time.
