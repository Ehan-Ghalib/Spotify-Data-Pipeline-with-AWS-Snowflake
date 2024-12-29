--=====================================================================================================================
--=====================================================================================================================

DESC integration s3_init;

--Add new storage location (list should have old & new though) to storage integration
ALTER STORAGE INTEGRATION S3_INIT
SET STORAGE_ALLOWED_LOCATIONS = ('s3://aws-src-files','s3://spotify-elt-pipelines/transformed-data/');

-- Creating external stage with the integration
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_s3_album_stg
    url='s3://spotify-elt-pipelines/transformed-data/albums-data/'
    STORAGE_INTEGRATION = S3_INIT
    FILE_FORMAT = FILE_FORMATS.CSV_FILE_FORMAT;

--list different files in the stage
LIST @MANAGE_DB.external_stages.aws_s3_album_stg;

--Create new schema for Spotify project
CREATE OR REPLACE SCHEMA MANAGE_DB.SPOTIFY; 

--Create table for albums
CREATE OR REPLACE TABLE MANAGE_DB.SPOTIFY.TOP100_ALBUMS_PARSED
(
    ALBUM_ID VARCHAR(100),
    ALBUM_NAME VARCHAR(100),
    RELEASE_DATE DATE,
    ALBUM_URI VARCHAR(500),
    TOTAL_TRACKS INT,
    TRACK_ID VARCHAR(100),
    INSERT_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

--test copy command: aws -> table
COPY INTO MANAGE_DB.SPOTIFY.TOP100_ALBUMS_PARSED
(
    ALBUM_ID,
    ALBUM_NAME,
    RELEASE_DATE,
    ALBUM_URI,
    TOTAL_TRACKS,
    TRACK_ID
)
FROM '@MANAGE_DB.external_stages.aws_s3_album_stg/album_transformed_2024-12-22 10:59:28.151601.csv'
FILE_FORMAT = (FORMAT_NAME = FILE_FORMATS.CSV_FILE_FORMAT ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR = 'CONTINUE';

SELECT * FROM MANAGE_DB.SPOTIFY.TOP100_ALBUMS_PARSED;

DESC TABLE MANAGE_DB.SPOTIFY.TOP100_ALBUMS_PARSED;

--TRUNCATE TABLE MANAGE_DB.SPOTIFY.TOP100_ALBUMS_PARSED;

--create pipe for employee feed
CREATE OR REPLACE PIPE MANAGE_DB.SPOTIFY.ALBUMS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO MANAGE_DB.SPOTIFY.TOP100_ALBUMS_PARSED
(
    ALBUM_ID,
    ALBUM_NAME,
    RELEASE_DATE,
    ALBUM_URI,
    TOTAL_TRACKS,
    TRACK_ID
)
FROM @MANAGE_DB.external_stages.aws_s3_album_stg
FILE_FORMAT = (FORMAT_NAME = FILE_FORMATS.CSV_FILE_FORMAT ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR = 'CONTINUE';

--=====================================================================================================================
--=====================================================================================================================

--create stage for ARTISTS
CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.AWS_S3_ARTIST_STG
URL='s3://spotify-elt-pipelines/transformed-data/artists-data/'
STORAGE_INTEGRATION = S3_INIT
FILE_FORMAT = FILE_FORMATS.CSV_FILE_FORMAT;

--create table for ARTISTS
CREATE OR REPLACE TABLE MANAGE_DB.SPOTIFY.TOP100_ARTISTS_PARSED
(
    ARTIST_ID VARCHAR(100),
    ARTIST_NAME VARCHAR(250),
    ALBUM_URI VARCHAR(500),
    TRACK_ID VARCHAR(100),
    INSERT_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

--test copy command: aws -> table, for ARTISTS data
COPY INTO MANAGE_DB.SPOTIFY.TOP100_ARTISTS_PARSED
(
    ARTIST_ID,
    ARTIST_NAME,
    ALBUM_URI,
    TRACK_ID
)
FROM '@MANAGE_DB.external_stages.AWS_S3_ARTIST_STG/artist_transformed_2024-12-22 10:59:28.255202.csv'
FILE_FORMAT = (FORMAT_NAME = FILE_FORMATS.CSV_FILE_FORMAT ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR ='CONTINUE';

SELECT * FROM MANAGE_DB.SPOTIFY.TOP100_ARTISTS_PARSED;

--Create pipe for ARTISTS
CREATE OR REPLACE PIPE MANAGE_DB.SPOTIFY.ARTISTS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO MANAGE_DB.SPOTIFY.TOP100_ARTISTS_PARSED
(
    ARTIST_ID,
    ARTIST_NAME,
    ALBUM_URI,
    TRACK_ID
)
FROM @MANAGE_DB.external_stages.AWS_S3_ARTIST_STG
FILE_FORMAT = (FORMAT_NAME = FILE_FORMATS.CSV_FILE_FORMAT ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR ='CONTINUE';

--=====================================================================================================================
--=====================================================================================================================

--create stage for TRACKS
CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.AWS_S3_TRACK_STG
URL='s3://spotify-elt-pipelines/transformed-data/tracks-data/'
STORAGE_INTEGRATION = S3_INIT
FILE_FORMAT = FILE_FORMATS.CSV_FILE_FORMAT;

--create table for TRACKS
CREATE OR REPLACE TABLE MANAGE_DB.SPOTIFY.TOP100_TRACKS_PARSED
(
    TRACK_ID VARCHAR(100),
    TRACK_NAME VARCHAR(250),
    TRACK_NUM INT,
    TRACK_DURATION INT,
    TRACK_URI VARCHAR(500), 
    TRACK_ADDED DATE,
    TRACK_POPULARITY INT,
    INSERT_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

--test copy command: aws -> table, for TRACKS data
COPY INTO MANAGE_DB.SPOTIFY.TOP100_TRACKS_PARSED
(
    TRACK_ID,
    TRACK_NAME,
    TRACK_NUM,
    TRACK_DURATION,
    TRACK_URI, 
    TRACK_ADDED,
    TRACK_POPULARITY
)
FROM '@MANAGE_DB.external_stages.AWS_S3_TRACK_STG/track_transformed_2024-12-22 10:59:27.880150.csv'
FILE_FORMAT = (FORMAT_NAME = FILE_FORMATS.CSV_FILE_FORMAT ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR ='CONTINUE';

SELECT * FROM MANAGE_DB.SPOTIFY.TOP100_TRACKS_PARSED;

--Create pipe for TRACKS
CREATE OR REPLACE PIPE MANAGE_DB.SPOTIFY.TRACKS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO MANAGE_DB.SPOTIFY.TOP100_TRACKS_PARSED
(
    TRACK_ID,
    TRACK_NAME,
    TRACK_NUM,
    TRACK_DURATION,
    TRACK_URI, 
    TRACK_ADDED,
    TRACK_POPULARITY
)
FROM @MANAGE_DB.external_stages.AWS_S3_TRACK_STG
FILE_FORMAT = (FORMAT_NAME = FILE_FORMATS.CSV_FILE_FORMAT ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR ='CONTINUE';

DESC PIPE MANAGE_DB.SPOTIFY.ALBUMS_PIPE;
DESC PIPE MANAGE_DB.SPOTIFY.ARTISTS_PIPE;
DESC PIPE MANAGE_DB.SPOTIFY.TRACKS_PIPE;

LIST @MANAGE_DB.EXTERNAL_STAGES.AWS_S3_TRACK_STG;

SELECT * FROM INFORMATION_SCHEMA.LOAD_HISTORY;

/*
SHOW PIPES IN DATABASE MANAGE_DB;
SELECT CURRENT_TIMESTAMP;
*/

--=====================================================================================================================
--=====================================================================================================================