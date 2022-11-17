import pandas as pd
import requests
import json
import math
#Important link for parent comments: https://developers.google.com/youtube/v3/docs/comments#id
#Important link for comment threads: https://developers.google.com/youtube/v3/docs/comments/list?apix_params=%7B%22part%22%3A%5B%22snippet%22%5D%2C%22maxResults%22%3A100%2C%22parentId%22%3A%22UgwXsDqMuiXOSHtV9p54AaABAg%22%7D&apix=true
#Refer postman for implementation
def getCommentsForVideo(video_id):
    comment_list = []
    count = 0
    comments_from = ''
    try:
        comments_from = requests.get(f'https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&maxResults=100&videoId={video_id}&key=AIzaSyAiBa23mUdvRRlkLGY8cSc7mzv-OQ-JMUY')
        print(type(comments_from.status_code))
        comment_json_data = json.loads(comments_from.text)
        if comments_from.status_code != 200:
            print(json.loads(comments_from.text))
            print('1',comments_from.status_code)
            print(f'{video_id} was the last one')
            return [], 0, False
        if 'items' in comment_json_data:
            for x in range(0,len(comment_json_data['items'])):
                comment_element = {
                    'id': comment_json_data['items'][x]['snippet']['topLevelComment']['id'],
                    'textOriginal': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'authorDisplayName': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'likeCount': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['likeCount'],
                    'publishedAt': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'updatedAt': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['updatedAt']
                }
                comment_list.append(comment_element)
            count = count+len(comment_json_data['items'])
            print(f'{count} processed',end="\r")
            while('nextPageToken' in comment_json_data):
                next_page_token = comment_json_data['nextPageToken']
                comments_from = requests.get(f'https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&maxResults=100&pageToken={next_page_token}&videoId={video_id}&key=AIzaSyAiBa23mUdvRRlkLGY8cSc7mzv-OQ-JMUY')
                comment_json_data = json.loads(comments_from.text)
                if comments_from.status_code != 200:
                    
                    print(json.loads(comments_from.text))
                    print('2',comments_from.status_code)
                    print(f'{video_id} was the last one')
                    return [], 0, False
                if 'items' in comment_json_data:
                    for x in range(0,len(comment_json_data['items'])):
                        comment_element = {
                            'id': comment_json_data['items'][x]['snippet']['topLevelComment']['id'],
                            'textOriginal': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['textOriginal'],
                            'authorDisplayName': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'likeCount': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['likeCount'],
                            'publishedAt': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['publishedAt'],
                            'updatedAt': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['updatedAt']
                        }
                        comment_list.append(comment_element)
                    count = count+len(comment_json_data['items'])
                    print(f'{count} processed',end="\r") 
        return comment_list,len(comment_list), True
    except(err):
        
        print(json.loads(comments_from.text))
        print('3',comments_from.status_code)
        print(f'{video_id} was the last one')
        return [], len(comment_list), False
       
    

#print(getCommentsForVideo('JrMiSQAGOS4'))

df_video_metadata = pd.read_csv(f'./Output_Dataset/df_video_metadata(0To10000).csv')
comment_cnts = list(df_video_metadata['video_comment_count'])
hit_cnts = 0
total_comments = 0
for cnt in comment_cnts[0:int(len(comment_cnts)/2)]:
    #if str(cnt) != 'Nan' or str(cnt) != 'nan' or str(cnt) != 'NaN':
    if not math.isnan(cnt):
        cnt = int(cnt)
        if int(cnt/100) == 0:
            hit_cnts+=1
        else:
            hit_cnts+=int(cnt/100)
        total_comments+=cnt

print(f'For {total_comments} comments we need to hit the API {hit_cnts} times')

video_comments = []
completed = True
prev_completed = True
abandoned_index = -1
video_ids = list(df_video_metadata['video_id'])
for idx, vid in enumerate(video_ids):
    if prev_completed:
        comments, len_comments, completed = getCommentsForVideo(vid)
        if completed:
            video_comments.append(comments)
            og_len = df_video_metadata['video_comment_count'][idx]
            print(f'Fetched {len_comments} out of {og_len}')
        else:
            print('Some error occured while fetching the comments...')
            video_comments.append([{'id': 'API Limit Exceeded',
                    'textOriginal': 'API Limit Exceeded',
                    'authorDisplayName': 'API Limit Exceeded',
                    'likeCount': 'API Limit Exceeded',
                    'publishedAt': 'API Limit Exceeded',
                    'updatedAt': 'API Limit Exceeded'}])
            abandoned_index = idx
            prev_completed = False
            print('Abandoning the rest of the dataset')
    else:
        video_comments.append([{'id': 'API Limit Exceeded',
                'textOriginal': 'API Limit Exceeded',
                'authorDisplayName': 'API Limit Exceeded',
                'likeCount': 'API Limit Exceeded',
                'publishedAt': 'API Limit Exceeded',
                'updatedAt': 'API Limit Exceeded'}])
if completed:
    print(f'Saving the dataset as TW-v_(0to10000)_NXT-ct.csv')
    df_video_metadata = df_video_metadata.assign(parent_comments=video_comments)
    df_video_metadata.to_csv(f'./Next/TW-v_(0to10000)_NXT-ct.csv',index=False)
else:
    print(f'Saving the dataset as TW-v_(0to10000)_INC-co({abandoned_index}).csv')
    df_video_metadata = df_video_metadata.assign(parent_comments=video_comments)
    df_video_metadata.to_csv(f'./Incomplete/TW-v_(0to10000)_INC-co({abandoned_index}).csv',index=False)