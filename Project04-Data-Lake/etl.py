import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format
from pyspark.sql.types import StructType as R, StructField as Fld, \
    DoubleType as Dbl, StringType as Str, IntegerType as Int, TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
        Description:
            Creates a Spark Session
        
        Arguments:
            None
            
        Returns:
            Spark Session Object
    """
        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration() \
        .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        Description:
            Performs the data ingest for song data from the S3 bucket, 
            then performs ETL process for creating songs and artists tables
        
        Arguments:
            spark:       The Spark Session Object
            input_data:  Path to S3 bucket containing input data
            output_data: Path to S3 bucket where the songs and artists tables
                         are stored in Parquet format
            
        Returns:
            None
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # Specify Schema for reading song data
    songdata_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, \
                         schema=songdata_schema, \
                         multiLine=True, \
                         mode='PERMISSIVE', \
                         columnNameOfCorruptRecord='corrupt_record')

    # extract columns to create songs table
    songs_table = df.select("song_id", \
                            "title", \
                            "artist_id", \
                            "year", \
                            "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite") \
                .partitionBy("year", "artist_id") \
                .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artist_cols = ["artist_id", \
                   "artist_name as name", \
                   "artist_location as location", \
                   "artist_latitude as latitude", \
                   "artist_longitude as longitude"]
    artists_table = df.selectExpr(artist_cols).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite") \
                    .parquet(os.path.join(output_data, 'artists'))
    
    # Create view for use in creating songplays table in log processing function
    df.createOrReplaceTempView('song_df_table')


def process_log_data(spark, input_data, output_data):
    
    """
        Description:
            Performs the data ingest for log data from the S3 bucket, then
            performs ETL process for creating users, artists
            and songplays tables
        
        Arguments:
            spark: The Spark Session Object
            input_data: Path to S3 bucket containing input data
            output_data: Path to S3 bucket where the users, artists and
            songplays tables are stored
            
        Returns:
            None
    """

    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    df = spark.read.json(log_data, \
                         multiLine=True, \
                         mode='PERMISSIVE', \
                         columnNameOfCorruptRecord='corrupt_record')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_cols = ["userId as user_id", \
                  "firstName as first_name", \
                  "lastName as last_name", \
                  "gender", \
                  "level"]
    users_table = df.selectExpr(users_cols).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite") \
                .parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("start_time", get_timestamp(col('ts')))
       
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time").cast(TimestampType()))) \
        .withColumn("day", dayofmonth(col("start_time").cast(TimestampType()))) \
        .withColumn("week", weekofyear(col("start_time").cast(TimestampType()))) \
        .withColumn("month", month(col("start_time").cast(TimestampType()))) \
        .withColumn("year", year(col("start_time").cast(TimestampType()))) \
        .withColumn("weekday", date_format(col("start_time").cast(TimestampType()), 'E'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite") \
                .partitionBy("year", "month") \
                .parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    # song_df = spark.read.parquet(output_data + 'songs/')
    song_df = spark.sql('SELECT DISTINCT song_id, \
                                        title, \
                                        artist_id, \
                                        artist_name, \
                                        duration \
                                        FROM song_df_table')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & \
                              (df.artist == song_df.artist_name) & \
                              (df.length == song_df.duration), \
                              how='left_outer') \
        .distinct() \
        .select(monotonically_increasing_id().alias("songplay_id"),
                 col("start_time"),
                 col("userId").alias("user_id"),
                 col("level"),
                 col("song_id"),
                 col("artist_id"),
                 col("sessionId").alias('session_id'),
                 col("location"),
                 col("userAgent").alias("user_agent"),
        ).withColumn("month", month(col("start_time"))) \
         .withColumn("year", year(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite") \
                    .partitionBy("year", "month") \
                    .parquet(os.path.join(output_data, 'songplays'))


def main():

    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    # output_data = "s3a://sparkify-jdf/"
    
    input_data = config['PATHS']['INPUT_DATA']
    output_data = config['PATHS']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
