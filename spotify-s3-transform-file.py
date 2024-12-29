import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data_raw):
    album_list = []
    for row in data_raw['items']:
        track_id = row['track']['id']
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        release_date = row['track']['album']['release_date']
        album_uri = row['track']['album']['external_urls']['spotify']
        total_tracks = row['track']['album']['total_tracks']
        album_element = {'album_id': album_id, 'album_name': album_name, 'release_date': release_date, 
                        'album_uri': album_uri, 'total_tracks': total_tracks, 'track_id': track_id}
        album_list.append(album_element)
    return album_list

def artist(data_raw):
    artist_list = []
    for track in data_raw['items']:
        track_id = track['track']['id']
        for artist in track['track']['artists']:
            artist_id = artist['id']
            artist_name = artist['name']
            artist_uri = artist['external_urls']['spotify']
            artist_element = {'artist_id': artist_id, 'artist_name': artist_name, 
                            'album_uri': artist_uri, 'track_id': track_id}
            artist_list.append(artist_element)
    return artist_list

def track(data_raw):
    track_list = []
    for track in data_raw['items']:
        track_id = track['track']['id']
        track_name = track['track']['name']
        track_track_num = track['track']['track_number']
        track_duration = track['track']['duration_ms']
        track_uri = track['track']['external_urls']['spotify']
        track_added = track['added_at']
        track_popularity = track['track']['popularity']
        track_element = {'track_id': track_id, 'track_name': track_name, 'track_track_num': track_track_num,
                        'track_duration': track_duration, 'track_uri': track_uri, 'track_added': track_added, 
                        'track_popularity': track_popularity}
        track_list.append(track_element)
    return track_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-elt-pipelines"
    Key = "raw-data/to-be-processed/"

    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        track_list = track(data)

        df_track = pd.DataFrame.from_dict(track_list)
        df_track = df_track.drop_duplicates(subset=['track_id'])
        df_track['track_added'] = pd.to_datetime(df_track['track_added'], format='mixed', errors='coerce')

        df_album = pd.DataFrame.from_dict(album_list)
        df_album = df_album.drop_duplicates(subset=['album_id','track_id'])
        df_album['release_date'] = pd.to_datetime(df_album['release_date'], format='mixed', errors='coerce')

        df_artist = pd.DataFrame.from_dict(artist_list)
        df_artist = df_artist.drop_duplicates(subset=['artist_id','track_id'])

        track_key = "transformed-data/tracks-data/track_transformed_" + str(datetime.now()) + ".csv"
        track_buffer = StringIO()
        df_track.to_csv(track_buffer, index=False)
        track_content = track_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=track_key, Body=track_content)

        album_key = "transformed-data/albums-data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        df_album.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_key = "transformed-data/artists-data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        df_artist.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw-data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }