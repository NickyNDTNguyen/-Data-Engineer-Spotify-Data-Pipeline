import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import boto3

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    link = 'https://open.spotify.com/playlist/37i9dQZF1DWZ7eJRBxKzdO'
    playlist_URI = link.split('/')[-1]
    data = sp.playlist_tracks(playlist_URI)
    
    filename = 'raw_spotify_' + str(datetime.now()) + '.json'
    
    client = boto3.client('s3')
    client.put_object(
        Bucket='etl-pipeline-spotify',
        Key='raw_data/to_processed/' + filename,
        Body=json.dumps(data)
        )
        
    glue = boto3.client('glue')
    gluejobname = 'spotify_etl_job'
    
    try:
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print('Job status: ', status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)