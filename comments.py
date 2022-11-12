import pandas as pd
import requests
import json
def getCommentsForVideo(video_id):
    comment_list = []
    count = 0
    comments_from = requests.get(f'https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&maxResults=100&videoId={video_id}&key=AIzaSyAiBa23mUdvRRlkLGY8cSc7mzv-OQ-JMUY')
    comment_json_data = json.loads(comments_from.text)
    if 'items' in comment_json_data:
        for x in range(0,len(comment_json_data['items'])):
            comment_element = {
                'id': comment_json_data['items'][x]['id'],
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
            if 'items' in comment_json_data:
                for x in range(0,len(comment_json_data['items'])):
                    comment_element = {
                        'id': comment_json_data['items'][x]['id'],
                        'textOriginal': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'authorDisplayName': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'likeCount': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['likeCount'],
                        'publishedAt': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['publishedAt'],
                        'updatedAt': comment_json_data['items'][x]['snippet']['topLevelComment']['snippet']['updatedAt']
                    }
                    comment_list.append(comment_element)
                count = count+len(comment_json_data['items'])
                print(f'{count} processed',end="\r")    
    return comment_list,count,len(comment_list)

print(getCommentsForVideo('JrMiSQAGOS4'))