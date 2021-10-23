import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth,\
    hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import types as t

config = configparser.ConfigParser()
config.read('dl.cfg')

key = config['AWS']['AWS_ACCESS_KEY_ID']
secret = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID'] = key
os.environ['AWS_SECRET_ACCESS_KEY'] = secret


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", key) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", secret) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function that reads raw songs data from s3,
    transforms and normalize it into 2 dimension tables:
    songs and artists.
    It then writes backs these tables into s3 as parquet files.
    :param spark: spark context for your cluster
    :param input_data (str): input s3 data folder path
    :param output_data (str): output s3 data folder path
    :return: None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id',
                             'title',
                             'artist_id',
                             'year',
                             'duration']).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id")\
        .parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df.select(['artist_id',
                               'artist_name',
                               'artist_location',
                               'artist_latitude',
                               'artist_longitude'])\
                      .withColumnRenamed("artist_name", "name") \
                      .withColumnRenamed("artist_location", "location") \
                      .withColumnRenamed("artist_latitude", "latitude") \
                      .withColumnRenamed("artist_longitude", "longitude") \
                      .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Function that reads raw logs data from s3,
    transforms and normalize it into 3 tables:
    dimension tables users, time and the 'fact' table songplay.
    It then writes backs these tables into s3 as parquet files.
    :param spark: spark context for your cluster
    :param input_data (str): input s3 data folder path
    :param output_data (str): output s3 data folder path
    :return: None
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    df.createOrReplaceTempView("logs")
    users_table = spark.sql("""
                            select
                                    userId as user_id,
                                    firstName as first_name,
                                    lastName as last_name,
                                    gender,
                                    level
                            from(
                                select
                                    userId,
                                    firstName,
                                    lastName,
                                    gender,
                                    level,
                                    row_number() 
                                    over(partition by userId 
                                         order by ts desc) as rk
                                from logs
                                ) as t
                            where t.rk = 1
                        """)

    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet")

    # create timestamp column from original timestamp column

    df = df.withColumn('ts2', df.ts/1000)
    df = df.withColumn('start_time',
                       date_format(df.ts2.cast(dataType=t.TimestampType()),
                                               "yyyy-MM-dd hh:mm:ss"))
    # get_timestamp = udf()
    # df =
    #
    # # create datetime column from original timestamp column
    # get_datetime = udf()
    # df =

    # extract columns to create time table
    time_table = df.select(['start_time'])\
                   .withColumn('hour', hour('start_time'))\
                   .withColumn('day', dayofmonth('start_time'))\
                   .withColumn('week', weekofyear('start_time'))\
                   .withColumn('month', month('start_time'))\
                   .withColumn('year', year('start_time'))\
                   .withColumn('weekday', dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month")\
        .parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create
    # songplays table
    df.createOrReplaceTempView("logs")
    songplays_table = spark.sql("""
        select
            logs.start_time,
            logs.userId as user_id,
            logs.level,
            songs.song_id,
            songs.artist_id,
            logs.sessionId as session_id,
            logs.location,
            logs.userAgent as user_agent
        from logs
        join songs
            on logs.song = songs.title""")
    songplays_table = songplays_table.withColumn('songplay_id',
                                                 monotonically_increasing_id())\
                                     .withColumn('year',
                                                 year('start_time'))\
                                     .withColumn('month',
                                                 month('start_time'))
        # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month")\
        .parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://myawsbucket-cat/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
