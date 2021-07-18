import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys
import requests
import json
import datetime

DATABASE = "airflow" 
USER = "airflow"
PASSWORD = "airflow"
HOST = "postgres"
PORT = "5432"
TOKEN = '' #place your token here 
engine = create_engine('postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'.format(user=USER,password=PASSWORD,host=HOST,port=PORT,database=DATABASE))

def spotify_etl_run():

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    # Convert time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
    recently_played=r.json()
 
    album_list = []
    for row in recently_played['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                        'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)
    

    artist_dict = {}
    id_list = []
    name_list = []
    url_list = []
    for item in recently_played['items']:
        for key,value in item.items():
            if key == "track":
                for data_point in value['artists']:
                    id_list.append(data_point['id'])
                    name_list.append(data_point['name'])
                    url_list.append(data_point['external_urls']['spotify'])

    artist_dict = {'artist_id':id_list,'name':name_list,'url':url_list}


    song_list = []
    for row in recently_played['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_time_played = row['played_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'date_time_played':song_time_played,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)

    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])

    artist_df = pd.DataFrame.from_dict(artist_dict)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])

    song_df = pd.DataFrame.from_dict(song_list)
    song_df['unique_identifier'] = song_df['song_id'] + "-" + song_df['date_time_played'].astype(str)
    song_df = song_df[['unique_identifier','song_id','song_name','duration_ms','url','popularity','date_time_played','album_id','artist_id']]

    song_df.to_sql("songs", engine, index=False,if_exists='replace')
    album_df.to_sql("album", engine, index=False,if_exists='replace')
    artist_df.to_sql("artist", engine, index=False,if_exists='replace')

    print("Added in database successfully")

# spotify_etl_run()
