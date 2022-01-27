# Project 5: Data Pipelines using Apache Airflow

## Overview
Sparkify, a music streaming company, needs to automate and monitor it's ETL pipelines from S3 to the Data Warehouse.  The event log data and song data are stored in JSON format in an S3 bucket at udacity-dend.  Apache Airflow was selected as the workflow management tool to automate the staging of the source data, creation of the Fact/Dimension tables and performance of data quality checks.

## Files in Project
- **README.md** - The file that is currently being read.  Markdown file used to document the project.
- **sparkify-dag.png** - A graphic that shows the task ordering for the DAG.  Linked to by the `README.md` file.

In the `airflow` folder:
- **create_tables.sql** - This file contains the statements used for creating the tables in Redshift Server before the DAG is run.

In the `airflow/dags` folder:
- **udac_example_dag.py** - This file contains the DAG for staging the source data, populating the Fact/Dimension tables and performing the quality check.

In the `airflow/plugins` folder:
- **\_\_init\_\_.py** - This file contains references to the files in `helpers` and `operators` folders for import into Airflow.

In the `airflow/plugins/helpers` folder:
- **\_\_init\_\_.py** - Contains the reference to the `sql_queries.py` file for import into Airflow.
- **sql_queries.py** - Contains the insert statements used for populating the Fact/Dimension tables.

In the `airflow/plugins/operators` folder:
- **\_\_init\_\_.py** - Contains the references to the custom operators for import into Airflow.
- **data_quality.py** - Contains the custom operator for performing a data quality check on the Fact/Dimension tables.
- **load_dimension.py** - Contains the custom operator for loading data from the staging tables into the Dimension tables.
- **load_fact.py** - Contains the custom operator for loading data from the staging tables into the Fact table.
- **stage_redshift.py** - Contains the custom operator for loading data from the S3 bucket into the staging tables.

## Staging Tables ETL process
The Staging tables are loaded from the S3 bucket using the `StageToRedshiftOperator` custom operator.  The operator uses a parameterized template to generate the copy statements for loading data from the S3 bucket to the staging tables.  The parameters used include:
- Redshift credentials
- AWS credentials
- Destination table
- S3 bucket
- S3 key
- AWS Region
- JSON format (defaults to auto or otherwise takes a JSON path to specify table structure)

The staging operator writes a log to Airflow for each table loaded.  Additionally, when the song data is encountered, the copy command is modified so that the timestamp is in milliseconds to match the source data.

## Fact Table ETL process
The `songplays` Fact Table is loaded from the staging_events and staging_songs tables using the `songplay_table_insert` statement referenced in the `sql_queries.py` file.  The `songplay_id` is generated using an MD5 on session id and start time, Null timestamped records are excluded and only records with song play events are included.

## Dimension Table ETL process
The dimension tables are loaded using the provided insert statements in the `sql_queries.py` with the `LoadDimensionOperator` operator.  The parameters used by the `LoadDimensionOperator` include:
- Redshift credentials
- Destination table
- SQL INSERT Statement - from the `sql_queries.py` file

The operator populates each of the Dimension table and logs the action taken in Airflow.

## Data Quality Check process
The data quality check looks for empty tables using a list of tables and logs each check to Airflow.

## Project Dataset Location
- Log (event) data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

## DAG Configuration Parameters
- Owner: 'jflowers'
- Start Date: January 12, 2019
- Dependency on past runs: False
- Retries: 3
- Retry Delay: 5 minutes
- Catchup: False
- Email on Retry: False
- Description: 'Load and transform data in Redshift with Airflow'
- Schedule Interval: '0 * * * *'
- Max Active Runs: 1

Here is the DAG task order:
![Sparkify DAG](./sparkify-dag.png)

## Instructions
1. Create a Redshift instance and setup up access
a. Setup user name and password for accessing Redshift, this will be used by Airflow
b. Make Redshift publicly accessible
c. Add security group that permits access to Redshift from Airflow
d. Add IAM role that is associated with the Access key and Secret key that will be used with Airflow
2. Start the Airflow server
a. Create a Redshift connection using the username and password created during Redshift setup
b. Create an AWS connection using the Access Key and Secret Key associated with the IAM role used by the Redshift server
3. Trigger the DAG to run
