import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = 's3://etl-pipeline-spotify/raw_data/to_processed/'
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json"
).toDF()

def process_album(df):
    df = source_df.select(explode("items").alias("items")).select(col("items.track.album.id").alias("album_id"), 
                                                                  col("items.track.album.name").alias("album_name"), 
                                                                  col("items.track.album.release_date").alias("album_release_date"), 
                                                                  col("items.track.album.total_tracks").alias("album_total_tracks"), 
                                                                  col("items.track.external_urls.spotify").alias("album_url"))\
                                                          .drop_duplicates(['album_id'])
    df = df.withColumn("album_release_date", col("album_release_date").cast("date"))
    return df


def process_artist(df):
    df = source_df.select(explode(col("items")).alias("items"))\
                  .select(explode(col("items.track.artists")).alias("artists"))\
                  .select(col("artists.id").alias("artist_id"),
                          col("artists.name").alias("artist_name"),
                          col("artists.href").alias("artist_url"))\
                  .drop_duplicates(['artist_id'])
    return df


def process_song(df):
    df = source_df.select(explode("items").alias("items")).select(col("items.track.id").alias("song_id"),
                                                                  col("items.track.name").alias("song_name"),
                                                                  col("items.track.duration_ms").alias("song_duration"),
                                                                  col("items.track.external_urls.spotify").alias("song_url"),
                                                                  col("items.track.popularity").alias("song_popularity"),
                                                                  col("items.added_at").alias("song_added"),
                                                                  col("items.track.album.id").alias("album_id"),
                                                                  col("items.track.artists.id").getItem(0).alias("artist_id"))\
                                                          .drop_duplicates(['song_id'])
    df = df.withColumn("song_added", col("song_added").cast("date"))
    return df
    
album_df = process_album(source_df)
artist_df = process_artist(source_df)
song_df = process_song(source_df)

def write_to_s3(df, path, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path":"s3://etl-pipeline-spotify/transformed_data/{}".format(path)},
        format=format_type)
        
write_to_s3(album_df, "album_data/album_transformed_{}".format(datetime.now().strftime("%Y%m%d%H%M%S")), "csv")
write_to_s3(artist_df, "artist_data/artist_transformed_{}".format(datetime.now().strftime("%Y%m%d%H%M%S")), "csv")
write_to_s3(song_df, "song_data/song_transformed_{}".format(datetime.now().strftime("%Y%m%d%H%M%S")), "csv")

def list_s3_objects(bucket, perfix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.json')]
    return keys
    
bucket_name = 'etl-pipeline-spotify'
prefix = 'raw_data/to_processed/'
spotify_json_files = list_s3_objects(bucket_name, prefix)

def move_and_delete_files(spotify_json_files, Bucket):
    s3_resource = boto3.resource('s3')
    for file in spotify_json_files:
        copy_source = {
            'Bucket': Bucket,
            'Key': file
        }
        destination_file = 'raw_data/processed/' + file.split('/')[-1]
        s3_resource.meta.client.copy(copy_source, Bucket, destination_file)
        s3_resource.Object(Bucket, file).delete()
        
move_and_delete_files(spotify_json_files, bucket_name)

job.commit()