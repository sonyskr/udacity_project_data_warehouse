## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

The project is written in `python` and uses `Amazon s3` for file storage and `Amazon Redshift`
 for database storage and data warehouse purpose.

## Source Data
The source data is in log files given the Amazon s3 bucket  at `s3://udacity-dend/log_data` 
and `s3://udacity-dend/song_data` containing log data and songs data respectively.

Log files contains songplay events of the users in json format 
while song_data contains list of songs details.

## Database Schema
Following are the fact and dimension tables made for this project:
#### Dimension Tables:
   * users
        * columns: user_id, first_name, last_name, gender, level
   * songs
        * columns: song_id, title, artist_id, year, duration
   * artists
        * columns: artist_id, name, location, lattitude, longitude
   * time
        * columns: start_time, hour, day, week, month, year, weekday
   
#### Fact Table:
   * songplays
        * columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## To run the project:
#### Setup Configurations 
Setup the dwh.cfg file (File not added in this repository). File format for **dwh.cfg**


#### Create tables

    $ python create_tables.py

#### Load Data

    $ python create_tables.py