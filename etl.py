import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, from_unixtime

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']["AWS_ACCESS_KEY_ID"]
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']["AWS_SECRET_ACCESS_KEY"]

def create_spark_session():
    '''
     Create the spark object with hadoop configuration.
    
    Parameters:
        None
        
    Returns:
        None
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Get the files from song folders and compose a DataFrame.
    Create the songs and artist tables with the desired columns.
    
    Parameters:
        spark (object): Previous created spark object.
        input_data(string): Key for AWS S3 objects to read.
        output_data(string): Key for AWS S3 objects to save.
        
    Returns:
        None
    '''
    # get filepath to song data file
    song_data = input_data+'song_data'
    
    # read song data file
    # Smaller data to test: s3a://{}:{}@udacity-dend/song_data/A/A/A/*.json
    df = spark.read.json("s3a://{}:{}@udacity-dend/song_data/*/*/*/*.json"\
                         .format(os.environ['AWS_ACCESS_KEY_ID'],os.environ['AWS_SECRET_ACCESS_KEY']))

    # extract columns to create songs table
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(*song_columns)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'/songs/', mode='overwrite', partitionBy=['year', 'artist_id'])
    
    # extract columns to create artists table
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df.select(*artist_columns)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'/artists/', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Get the files from log folders and compose a DataFrame.
    Create the users, time and songplays tables with
    the desired columns and format.
    
    Parameters:
        spark (object): Previous created spark object.
        input_data(string): Key for AWS S3 objects to read.
        output_data(string): Key for AWS S3 objects to save.
        
    Returns:
        None
    '''
    # get filepath to log data file
    log_data = input_data+'log_data'

    # read log data file
    # smaller data to test: s3a://{}:{}@udacity-dend/log_data/2018/11/2018-11-12*.json
    df = spark.read.json("s3a://{}:{}@udacity-dend/log_data/*/*/*.json"\
                      .format(os.environ['AWS_ACCESS_KEY_ID'],os.environ['AWS_SECRET_ACCESS_KEY']))
    
    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')

    # extract columns for users table
    users_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df.select(*users_columns).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'/users', mode='overwrite')
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', from_unixtime(col('ts')/1000))
    
    # extract columns to create time table
    df_time = df.select('datetime').dropDuplicates()
    time_table = df_time.withColumnRenamed('datetime', 'start_time')\
                         .orderBy('start_time', ascending=True)\
                         .withColumn('hour', hour(col('start_time')))\
                         .withColumn('day', dayofmonth(col('start_time')))\
                         .withColumn('week', weekofyear(col('start_time')))\
                         .withColumn('month', month(col('start_time')))\
                         .withColumn('year', year(col('start_time')))\
                         .withColumn('weekday', dayofweek(col('start_time')))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'/time', mode='overwrite', partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    basePath= output_data+'/songs/'
    song_df = spark.read.option("basePath",basePath).parquet(output_data+'/songs/*')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='left')
    songplays_table = songplays_table.drop('song', 'artist', 'title', 'year', 'duration')
    
    columns_name = ['start_time', 'user_id', 'level', 'session_id', 'location', 'user_agent', 'song_id', 'artist_id']
    songplays_table = songplays_table.toDF(*columns_name)
    songplays_table = songplays_table.withColumn('month', month(col('start_time')))\
                                     .withColumn('year', year(col('start_time')))

   # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'/songplays', 
                                  mode='overwrite', 
                                  partitionBy=['year', 'month'])


def main():
    '''
    Create the spark session.
    Has the parameters for input and output folders.
    Call the functions to create tables from song files
    and from log files.
    
    Parameters:
        None
        
    Returns:
        None
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://misho-udacity-bucket/datalake"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
