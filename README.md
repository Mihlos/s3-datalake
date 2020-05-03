# DataLake S3
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.

Sparkify wants an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# Database schema
The database has a star schema composed by one fact table called songplays and four dimensional tables (users, songs, artists, time).
The benefits of the schema are:
* Query performance
* Because a star schema database has a small number of tables and clear join paths, queries run faster.
* Load performance and administration
* Structural simplicity also reduces the time required to load large batches of data into a star schema database. 
* Easily understood

**Fact Table**
*songplays* - records in log data associated with song plays i.e. records with page NextSong
fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
*users* - users in the app. 
fields: user_id, first_name, last_name, gender, level
*songs* - songs in music database
fields: song_id, title, artist_id, year, duration
*artists* - artists in music database
fields: artist_id, name, location, latitude, longitude
*time* - timestamps of records in songplays broken down into specific units
fields: start_time, hour, day, week, month, year, weekday

# ETL Pipeline
1. Obtain the files from S3 song folders.
2. Compose a Dataframe with all of them.
3. Create the songs and artists tables with the desired columns.
4. Save to S3 in the desired output folders. One folder for table.
    a. songs partitioned by year and artist
4. Obtain the files from S3 log folders.
5. Compose a Dataframe with all of them.
6. Create the users, time and songplays tables.
7. Save to S3 in the desired output folders. One folder for table.
    a. time partitioned by year and month
    b. songplays partitioned by year and month