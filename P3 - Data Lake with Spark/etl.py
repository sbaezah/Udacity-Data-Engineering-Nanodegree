import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType



config = configparser.ConfigParser()
config.read('dl.cfg') 

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """ The "create_spark_session" function will create the spark session, and use during the entire process execution """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This function will get the data from Udacity s3 bucket available for this project. Extract data from the song_data path, select the columns that the project requires and create the output tables in parquet files for the artist and song table."""
    
    
    print("""##### Reading song data from json files #####
    """)
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    
    # read song data file
    df = spark.read.load(song_data, format="json").dropDuplicates()
    
    print(df.show(1))
    # extract columns to create songs table
    songs_table = df.select("song_id","title", "artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    print("""##### [STARTING] Writing table to the parquet files: 
                   SONGS #####
                   """)
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+"songs")
    print("""##### [FINISHED] Table SONGS already loaded
                   #####""")      

    # extract columns to create artists table
    artists_table = df.select("artist_id",col("artist_name").alias("name"),col("artist_location").alias("location"),col("artist_latitude").alias("latitude"),col("artist_longitude").alias("longitude")).dropDuplicates()
    
    # write artists table to parquet files
    print("""##### [STARTING] Writing table to the parquet files: 
                   ARTISTS #####
                   """)
    artists_table.write.mode("overwrite").parquet(output_data+"artists")
    print("""##### [FINISHED] Table ARTISTS already loaded #####
    """)  


def process_log_data(spark, input_data, output_data):
    """This function will get the data from Udacity s3 bucket available for this project. Extract data from the log_data path, select the columns that the project requires and create the output tables in parquet files for the artist and song table."""

    # get filepath to log data file
    log_data = input_data + "log-data"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col("page")=='NextSong').filter(df.userId.isNotNull())

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    print("""##### [STARTING] Writing table to the parquet files: 
                   USERS #####
                   """)
    users_table.write.mode("overwrite").parquet(output_data+"users")
    print("""##### [FINISHED] Table USERS already loaded #####
    """)  

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp",get_timestamp(col("ts"))) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(col("ts"))) 
    
    # extract columns to create time table
    time_table = df.select(
         'timestamp',
         hour('datetime').alias('hour'),
         dayofmonth('datetime').alias('day'),
         weekofyear('datetime').alias('week'),
         month('datetime').alias('month'),
         year('datetime').alias('year'),
         date_format('datetime', 'F').alias('weekday')
     )
    
    # write time table to parquet files partitioned by year and month
    print("""##### [STARTING] Writing table to the parquet files: 
                   TIME #####
                   """)
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"time")
    print("""##### [FINISHED] Table TIME already loaded #####
    """)  

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    # Creating a string variable with the timestamp format
    tsFormatVar = "yyyy/MM/dd HH:MM:ss z"

    '''In this part, the songplays table are made with a join between 2 dataframes, and after that the columns select with the transformation (if the transformation applies)'''      
    songplays_table = song_df.join(df,(song_df.artist_name==df.artist) & (song_df.title==df.song)).withColumn("songplay_id",monotonically_increasing_id()).withColumn('start_time', to_timestamp(date_format((col("ts") /1000).cast(dataType=TimestampType()), tsFormatVar),tsFormatVar)).select("songplay_id","start_time",col("userId").alias("user_id"),"level","song_id","artist_id",col("sessionId").alias("session_id"),col("artist_location").alias("location"),"userAgent",month(col("start_time")).alias("month"),year(col("start_time")).alias("year"))
    


    # write songplays table to parquet files partitioned by year and month
    print("""##### [STARTING] Writing table to the parquet files: 
                   SONGPLAYS #####
                   """)
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays")
    print("""##### [FINISHED] Table SONGPLAYS already loaded #####
    """)  

def process_report_data(spark, output_data):
    '''This extra function, was made for get and show the data load results, in this version only show the count rows per table.'''
    
    print("##### Total rows loaded in table SONGPLAYS #####") 
    df3 = spark.read.load(output_data+"/songplays/*/*/*.parquet")
    df3.groupBy().count().show()
    
    print(" #####################################################") 
    
    print("##### Total rows loaded in table USERS #####") 
    df3 = spark.read.load(output_data+"/users/*.parquet")
    df3.groupBy().count().show()
    
    print("#####################################################")
    
    print("##### Total rows loaded in table SONGS #####") 
    df3 = spark.read.load(output_data+"/songs/*/*/*.parquet")
    df3.groupBy().count().show()
    
    print(" #####################################################  ")
    
    print(" ##### Total rows loaded in table ARTISTS ##### ") 
    df3 = spark.read.load(output_data+"/artists/*.parquet")
    df3.groupBy().count().show()
    
    print(" ##################################################### ")
    
    print("##### Total rows loaded in table TIME #####") 
    df3 = spark.read.load(output_data+"/time/*/*/*.parquet")
    df3.groupBy().count().show()
    
    print(" ##################################################### ")
    
def main():
    """This is the main function that will call other 3 to make the ETL process.
       First calls the function "create spark session". This function creates the spark session that will be
       use during the entire process.
       Next the process execute the functions that read, transform and load the tables in parquet files"""
    
    print("""########### Udacity Data Engineer Nanodegree - Project 4 - Sparkify Data Lake ###########
             ########### Sebastian Baeza                                                   ###########
             #                                                                                       #
             # Starting the etl.py script                                                            #
             
             """)
    
    print("""######## [STARTING]: spark creation session object #########""")
    spark = create_spark_session()
    print("""######## [FINISHED]: spark creation session object #########""")
    
    '''The folder to setup the data extract, but for development stage, have to stay commented'''
    
    #comment the input data from "S3" and uncomment the input data with the local folder if you wish to
    #test with less amount of data yout code 
    #input_data = "/home/workspace/data/"
    input_data = "s3a://udacity-dend/"
    
    #I tried with my S3 location to output the parquet files, but was to slow, even for the small data test
    output_data = "/home/workspace/spark-datalake/"
    #output_data = "s3a://aws-logs-133335681780-us-west-2/spark-warehouse/"
    
    
    print("""######## [STARTING]: song data load process #########
          """)
    process_song_data(spark, input_data, output_data)    
    
    print("""######## [FINISHED]: song data load process #########
          """)
    
    print("""######## [STARTING]: LOG data load process #########
          """)    
    process_log_data(spark, input_data, output_data)
    
    print("""######## [FINISHED]: LOG data load process #########
          """)
     
    print("""######## [STARTING]: Report data loaded #########
          """)
    process_report_data(spark, output_data)
    
    print("""######## [FINISHED]: Report data loaded #########
          """)   

if __name__ == "__main__":
    """ that will call function: Main """
    main()