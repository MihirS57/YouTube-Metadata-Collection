import requests
import json
from datetime import datetime
from tqdm import tqdm 
import pandas as pd
import numpy as np
import json 

#YT_API_KEY = 'AIzaSyCIhhHAyeTzaTZcgB7aPN09Zj4-liSnATM'

'''
    Given a string, representing a YouTube URL, the function returns:
        - the URL type, i.e. 'video', 'channel' or 'other'
        - the ID of the URL
        
    YT_API_KEY is the YouTube developer API
'''
def getIDfromURL(yt_url, YT_API_KEY):
    try:
        link_type = ''
        link_id = ''
        if '/c/' in yt_url or '/channel/' in yt_url:
            if '/c/' in yt_url:
                c_index = yt_url.index('/c/')
                sub_yt_url = yt_url[c_index+3:]
                if '/' in sub_yt_url:
                    slash_index = sub_yt_url.index('/')+c_index+3
                    link_type = 'channel'
                    link_id = yt_url[c_index+3:slash_index]
                elif '?' in sub_yt_url:
                    quest_index = sub_yt_url.index('?')+c_index+3
                    link_type = 'channel'
                    link_id = yt_url[c_index+3:quest_index]                            
                else:
                    link_type = 'channel'
                    link_id = yt_url[c_index+3:]
                if len(link_id) != 24:
                    #make a channel id request
                    channelid_response = requests.get(f'https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&fields=items%2Fsnippet%2FchannelId&q={link_id}&key={YT_API_KEY}')
                    channelid_json = json.loads(channelid_response.text)
                    if 'items' in channelid_json and len(channelid_json['items']) > 0:
                        if pd.isna(channelid_json['items'][0]['snippet']['channelId']):
                            print(f'true NA {link_id}')
                            link_id='NA'
                        else:
                            link_id = channelid_json['items'][0]['snippet']['channelId']
                    else:
                        if channelid_json.status_code == 403:
                            print(f'Quota Exceeded, ending ops')
                        link_id='NA'
            elif '/channel/' in yt_url:
                c_index = yt_url.index('/channel/')
                sub_yt_url = yt_url[c_index+9:]
                if '/' in sub_yt_url:
                    slash_index = sub_yt_url.index('/')+c_index+9
                    link_type = 'channel'
                    link_id = yt_url[c_index+9:slash_index]
                elif '?' in sub_yt_url:
                    quest_index = sub_yt_url.index('?')+c_index+9
                    link_type = 'channel'
                    link_id = yt_url[c_index+9:quest_index]  
                else:
                    link_type = 'channel'
                    link_id = yt_url[c_index+9:]
        elif '/watch?v=' in yt_url or '/v=' in yt_url or '/shorts/' in yt_url or '.be/' in yt_url:
            if '/watch?v=' in yt_url:
                w_index = yt_url.index('/watch?v=')
                link_type = 'video'
                link_id = yt_url[w_index+9:w_index+20]
            elif '/v=' in yt_url:
                w_index = yt_url.index('/v=')
                link_type = 'video'
                link_id = yt_url[w_index+3:w_index+14]
            elif '/shorts/' in yt_url:
                w_index = yt_url.index('/shorts/')
                link_type = 'video'
                link_id = yt_url[w_index+8:w_index+19]  
            elif '.be/' in yt_url:
                w_index = yt_url.index('.be/')
                link_type = 'video'
                link_id = yt_url[w_index+4:w_index+15]
        else:
            com_index = yt_url.index('com/')
            if (len(yt_url)-(com_index+4)) == 11:
                link_type = 'video'
                link_id = yt_url[com_index+4:com_index+15]
            else:
                link_type = 'other'
                link_id = 'NA'
    except:
        return None, None
    
    return link_type,link_id

'''
    Given a list object, the function generates an interator over 'n' elements
'''
def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]
        
        
def get_video_metadata(yt_handler, ids, verbose= False):
    
    results,problematic_ids = [], []
    
    try:
        if verbose:
            for btc in tqdm(batch(ids,n=50)):
                results.append(yt_handler.get_video_metadata(btc,part=['statistics', 'snippet']))
        else:
            for btc in batch(ids,n=50):
                results.append(yt_handler.get_video_metadata(btc,part=['statistics', 'snippet']))           
    except Exception:
        problematic_ids.append(btc)

    results = [item for sublist in results for item in sublist]
    problematic_ids = [item for sublist in problematic_ids for item in sublist]

    for problematic_id in problematic_ids:
        try:
            metadata = yt_handler.get_video_metadata(problematic_id,
                                             part=['statistics', 'snippet'])
            if len(metadata) != 0:
                results.append(metadata)
        except:
            pass   
    
    df_video_metadata = pd.DataFrame.from_records(results)
    df_video_metadata = preprocess_video_metadata(df_video_metadata)
    
    return df_video_metadata
    
def get_channel_metadata(yt_handler, ids, verbose= False):
    
    results_c,problematic_ids_c = [], []
    
    try:
        if verbose:
            for btc in tqdm(batch(ids,n=50)):
                results_c.append(yt_handler.get_channel_metadata(btc))
        else:
            for btc in batch(ids,n=50):
                results_c.append(yt_handler.get_channel_metadata(btc))            
    except Exception:
        problematic_ids.append(btc)        

    results_c = [item for sublist in results_c for item in sublist]
    problematic_ids_c = [item for sublist in problematic_ids_c for item in sublist]
    
    df_channel_metadata = pd.DataFrame.from_records(results_c)
    df_channel_metadata = preprocess_channel_metadata(df_channel_metadata)
    return df_channel_metadata


def preprocess_video_metadata(df_video_metadata):

    df_video_metadata['video_publish_date'] = df_video_metadata['video_publish_date'].apply(lambda timestamp: datetime.fromtimestamp(timestamp))
    df_video_metadata['video_category'] = df_video_metadata['video_category'].astype(float)
    df_video_metadata['video_view_count'] = df_video_metadata['video_view_count'].astype(float)
    df_video_metadata['video_comment_count'] = df_video_metadata['video_comment_count'].astype(float)
    df_video_metadata['video_like_count'] = df_video_metadata['video_like_count'].astype(float)
    df_video_metadata['video_dislike_count'] = df_video_metadata['video_dislike_count'].astype(float)
    df_video_metadata['video_tags'] = df_video_metadata['video_tags'].apply(lambda x: x.split('|'))
    
    return df_video_metadata

def preprocess_channel_metadata(df_channel_metadata):
    
    df_channel_metadata['account_creation_date'] = df_channel_metadata['account_creation_date'].apply(lambda timestamp: datetime.fromtimestamp(timestamp))
    df_channel_metadata['view_count'] = df_channel_metadata['view_count'].astype(float)
    df_channel_metadata['video_count'] = df_channel_metadata['video_count'].astype(float)
    df_channel_metadata['subscription_count'] = df_channel_metadata['subscription_count'].astype(float)
    df_channel_metadata['topic_ids'] = df_channel_metadata['topic_ids'].apply(lambda x: x.split('|') if x is not None else [])    
    
    return df_channel_metadata
