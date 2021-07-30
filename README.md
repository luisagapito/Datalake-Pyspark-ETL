# Sparkify Data Modeling


This data modeling and ETL pipeline aim to allow the analytics team to understand what songs users are listening to.

## Database Schema

The data model consists of a **star schema** that has one fact table and 4 dimension tables. The fact table is the songplays table that has the records in log data associated with song plays. The dimensional tables are the users, songs, artists, and time tables. The users table has the information of users in the app. The songs table has the songs in music database. The artists table has the artist in music database. The time table has the timestamps of records in songplays broken down into specific units. This model was chosen because of some key required benefits for this project like fast aggregations, simplified queries, and denormalization.

## ETL Pipeline

The ETL pipeline starts creating the spark session. Then, it processes the log and song JSON files to create the dimensional and fact tables in parquet format in another S3 bucket. It converts the fields to the appropiate data type when needed. The songs and artists tables come from the *song_data* directory. The users and time tables come from the *log_data* directory. Finally, *song_data* and *log_data* directories are used to create views. The join of these views results in the the songplays fact table.I suggest automatizing this ETL pipeline in a daily batch mode in non-working hours so that the datalake is always up to date.

## Database and analytical goals

The main goal is to empower the analytics team will be able to query current data in a fast and optimized manner, letting them gather all the information required to make powerful decisions based on a great amount of data. For example, they can decide to invest more in a certain type of music in a specific season of the year because they know what songs users are listening to by that time.

## Usage

You can execute the *Project.ipynb* that runs the ETL pipeline converting the raw JSON data in an initial bucket into parquet files in another bucket. Most of the scripts have comments for a better understanding of what they are doing.

## Files Explanation

* **etl.py** : This Python file creates the Spark session. Then, it processes the JSON files in an S3 bucket to create the dimensional and fact tables in Parquet files, so they can be stored in another S3 bucket with partitions when needed.
* **dl.cfg** : This file contains the AWS Access and Secret keys.
* **Project.ipynb** : This Jupyter Notebook runs the etl.py to test it is working properly.