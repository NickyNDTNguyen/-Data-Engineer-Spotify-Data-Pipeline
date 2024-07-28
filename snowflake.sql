CREATE DATABASE spotify_db;

-- Create a secure connection with AWS S3
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::905418382065:role/spotify-spark-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://etl-pipeline-spotify/')
  COMMENT = 'Initialize connection between Snowflake and AWS S3';

DESC INTEGRATION s3_integration;


-- Create format file
CREATE OR REPLACE file format spotify_db.PUBLIC.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' ;


-- Create stage as a temporary storage that contains the data from S3 will be poured into
CREATE OR REPLACE STAGE spotify_db.PUBLIC.spotify_stage
    URL = 's3://etl-pipeline-spotify/transformed_data/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = spotify_db.PUBLIC.csv_fileformat
    COMMENT = 'It is considered as a temporary storage that contains the data from S3 will be poured into';

LIST @spotify_db.PUBLIC.spotify_stage;


-- Define tables
CREATE OR REPLACE TABLE spotify_db.PUBLIC.tb_album (
    album_id STRING,
    album_name STRING,
    album_release_date DATE,
    album_total_tracks INT,
    album_url STRING
);

CREATE OR REPLACE TABLE spotify_db.PUBLIC.tb_artist (
    artist_id STRING,
    artist_name STRING,
    artist_url STRING
);

CREATE OR REPLACE TABLE spotify_db.PUBLIC.tb_song (
    song_id STRING,
    song_name STRING,
    song_duration INT,
    song_url STRING,
    song_popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);


-- Load data into those tables to test
COPY INTO spotify_db.PUBLIC.tb_song
FROM @spotify_db.PUBLIC.spotify_stage/song_data/;

SELECT COUNT(*) FROM spotify_db.PUBLIC.tb_song;
TRUNCATE TABLE spotify_db.PUBLIC.tb_album;


-- Create Snowpipe to load data automatically
CREATE OR REPLACE SCHEMA spotify_db.pipes;

CREATE OR REPLACE pipe spotify_db.pipes.album_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.PUBLIC.tb_album
FROM @spotify_db.PUBLIC.spotify_stage/album_data/;

DESC pipe spotify_db.pipes.album_pipe;

------
CREATE OR REPLACE pipe spotify_db.pipes.artist_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.PUBLIC.tb_artist
FROM @spotify_db.PUBLIC.spotify_stage/artist_data/;

DESC pipe spotify_db.pipes.artist_pipe;

------
CREATE OR REPLACE pipe spotify_db.pipes.song_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.PUBLIC.tb_song
FROM @spotify_db.PUBLIC.spotify_stage/song_data/;

DESC pipe spotify_db.pipes.song_pipe;