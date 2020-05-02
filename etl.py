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
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data'
    
    # read song data file
    # I had problems with auth, this way works but I'm not using song_data var.
    # Smaller data because with all data it takes too long
    df = spark.read.json("s3a://{}:{}@udacity-dend/song_data/A/A/*/*.json"\
                         .format(os.environ['AWS_ACCESS_KEY_ID'],os.environ['AWS_SECRET_ACCESS_KEY']))
#     df = spark.read.json(song_data)

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
    # get filepath to log data file
    log_data = input_data+'log_data'

    # read log data file
    df = spark.read.json("s3a://{}:{}@udacity-dend/log_data/*/*/*.json"\
                      .format(os.environ['AWS_ACCESS_KEY_ID'],os.environ['AWS_SECRET_ACCESS_KEY']))
    
    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')

    # extract columns for users table
    users_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df.select(*users_columns)
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'/users', mode='overwrite')

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
    df = df.withColumn('datetime', from_unixtime(col('ts')/1000))
    
#     # extract columns to create time table
    df_time = df.select('datetime')
    time_table = df_time.withColumnRenamed('datetime', 'start_time')\
                         .orderBy('start_time', ascending=True)\
                         .withColumn('hour', hour(col('start_time')))\
                         .withColumn('day', dayofmonth(col('start_time')))\
                         .withColumn('week', weekofyear(col('start_time')))\
                         .withColumn('month', month(col('start_time')))\
                         .withColumn('year', year(col('start_time')))\
                         .withColumn('weekday', dayofweek(col('start_time')))

#     # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'/time', mode='overwrite', partitionBy=['year', 'month'])

#     # read in song data to use for songplays table
#     song_df = 

#     # extract columns from joined song and log datasets to create songplays table 
#     songplays_table = 

#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://misho-udacity-bucket/datalake"
    
#     process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
