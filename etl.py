"""
The script etl.py creates the spark session, processes the log and song JSON files to create the dimensional and fact tables in parquet format in another S3 bucket. 
"""

#Import libraries
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F


# Import AWS keys
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    The create_spark_session method creates the spark session.
    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    The process_song_data method processes the song data to create the songs and artists parquet files in another S3 bucket. The songs table is partitioned by year and         artist_id.
    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
        
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select([df.song_id,df.title,df.artist_id,df.year.cast("int"), \
                             df.duration.cast("double")])
    
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    song_data_output = output_data + "songs"
    songs_table.write.partitionBy(['year','artist_id']).parquet(song_data_output)
    
    # extract columns to create artists table
    table_artists = df.select([df.artist_id, \
      df.artist_name.alias('name'), df.artist_location.alias('location'), \
      df.artist_latitude.alias('latitude').cast("float"), \
      df.artist_longitude.alias('longitude').cast("float")])
    
    table_artists = table_artists.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_data_output = output_data + "artists"
    table_artists.write.parquet(artists_data_output)


def process_log_data(spark, input_data, output_data):
    """
    The process_log_data method processes the log data to create the users and time parquet files in another S3 bucket. The time and songplays tables     are partitioned by year and month. The songplays table is created joining the song and log tables.
    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select([df.userId.alias('user_id').cast("int"), \
      df.firstName.alias('first_name'), df.lastName.alias('last_name'), \
      df.gender, df.level])
    users_table = users_table.drop_duplicates(subset=['user_id'])
    
    # write users table to parquet files
    users_data_output = output_data + "users"
    users_table.write.parquet(users_data_output)

    # create timestamp column from original timestamp column
    get_start_time = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_start_time(df.ts))
    
    # extract columns to create time table
    table_time = df.select(df.start_time.cast("timestamp"), \
        hour(df.start_time).alias('hour').cast("int"),\
        dayofmonth(df.start_time).alias('day').cast("int"),\
        weekofyear(df.start_time).alias('week').cast("int"), \
        month(df.start_time).alias('month').cast("int"),\
        year(df.start_time).alias('year').cast("int"), \
        dayofweek(df.start_time).alias('weekday').cast("int"))
    
    table_time = table_time.drop_duplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_data_output = output_data + "time"
    table_time.write.partitionBy(['year','month']).parquet(time_data_output)

    # read in song data to use for songplays table
    song_data = input_data + "song_data/A/A/A/*.json"
    df_songs = spark.read.json(song_data)
    
    # Creating logs and songs views to use them in the next join
    df.createOrReplaceTempView('table_logs')
    df_songs.createOrReplaceTempView('table_songs')

    # extract columns from joined song and log datasets to create songplays table 
    song_plays_table = spark.sql("""select table_logs.start_time ,
    year(table_logs.start_time) year, month(table_logs.start_time) month,
    table_logs.userId as user_id, table_logs.level,
    table_songs.song_id, table_songs.artist_id,
    table_logs.sessionId as session_id, table_logs.location,
    table_logs.userAgent as user_agent from table_logs 
    join table_songs on (table_logs.artist = table_songs.artist_name and
    table_logs.song = table_songs.title and 
    table_logs.length = table_songs.duration )""").withColumn("songplay_id", F.monotonically_increasing_id())
    
    songplays = song_plays_table.select(song_plays_table.songplay_id.cast("int"), \
                                        song_plays_table.month.cast("int"), \
                                        song_plays_table.year.cast("int"), \
                                        song_plays_table.start_time.cast("timestamp"), \
                                        song_plays_table.user_id.cast("int"), \
                                        song_plays_table.level, \
                                        song_plays_table.song_id, \
                                        song_plays_table.artist_id, \
                                        song_plays_table.session_id.cast("int"), \
                                        song_plays_table.location, \
                                        song_plays_table.user_agent)

    
    # write songplays table to parquet files partitioned by year and month
    songplays_data_output = output_data + "songplays"
    songplays.write.partitionBy(['year','month']).parquet(songplays_data_output)


def main():
    """
    The main method calls methods to create the spark session, to process the song data and to process the log data.
    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-functional/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    """
    It calls the main method.

    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
    main()
