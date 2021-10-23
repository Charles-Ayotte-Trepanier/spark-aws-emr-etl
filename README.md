# Project 1: Data Modeling with Postgres

## Summary of the project
### Inroduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a S3 database using a Spark pipeline, with schema designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
### Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from raw S3 files in an analytical S3 directory using Spark. 

## Schema
### Fact Table

    
- **songplays** - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- **users** - users in the app
        user_id, first_name, last_name, gender, level
- **songs** - songs in music database
        song_id, title, artist_id, year, duration
- **artists** - artists in music database
        artist_id, name, location, latitude, longitude
- **time** - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

## Explanation of the files in the repository
- **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
- **etl.py** reads and processes raw songs and logs data, and loads the transformed schema into an analytical S3 bucket.
- **dl.cfg** contains the aws aim key/secret
- **create_emr.sh** contains the aws aim key/secret