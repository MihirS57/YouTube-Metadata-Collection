import pandas as pd
import json
import requests
bridge_file = open('./Bridge.txt','r')
key_list = bridge_file.readlines()
YT_API_KEY = key_list[0]

def readDatasetWithPath(path):
    return pd.read_csv(path,low_memory=False)
def getIDfromURL(yt_url):
    channelid_json = ''
    channelid_response = ''
    try:
        if 'youtube.com' in yt_url or 'youtu.be' in yt_url:
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
                            if channelid_response.status_code == 403:
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
            return link_type, link_id
        else:
            print(f'Posible non-YouTube link: {yt_url}')
            return 'other', 'NA'
    except Exception as e:
        print(f'Error prone: {yt_url}')
        print(f'Error caused: {e}')
        return 'error', channelid_json

def segregateVideosAndChannels(url_list):
    video_list = []
    channel_list = []
    count = 0
    len_urls = len(url_list)
    link_type, link_id = '',''
    for url in url_list:
        link_type, link_id = getIDfromURL(url)
        if link_type == 'video':
            video_list.append(link_id)
            count = count+1
        elif link_type == 'channel':
            channel_list.append(link_id)    
            count = count+1
        elif link_type == 'error':
            print(link_id)
            print('Some error occured while getting IDs from URLs...')
            break
        print(f'{count}/{len_urls}',end="\r")
    return video_list, channel_list, count
    video_list = pd.Series(video_list).value_counts().keys().tolist() 
    channel_list = pd.Series(channel_list).value_counts().keys().tolist() 
    return video_list, channel_list, count   
    
    return link_type,link_id
dataset_src = input('Dataset Name: (Include .csv) ')
dataset_org = input('Origin of dataset: (FB for Facebook & TW for Twitter) ')
dataset_sub = readDatasetWithPath(f'./Input_Dataset/{dataset_src}')
url_column_name = input('Name of the table column which stores the YouTube URLs: ')
dataset_url_list = list(dataset_sub[url_column_name])
dataset_videos, dataset_channel, yt_count = segregateVideosAndChannels(dataset_url_list)
print(f'Total YouTube links in dataset: {yt_count}, and size of dataset is: {len(dataset_url_list)}')


