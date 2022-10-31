'''
    python main.py --input "./yt_onlyurl.csv-modified.csv" --api_key "AIzaSyAiBa23mUdvRRlkLGY8cSc7mzv-OQ-JMUY" [--verbose]
'''


import argparse
import pandas as pd
import numpy as np
import requests
import os

from youtube_api import YouTubeDataAPI

import utils

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input', type=str, help="This should be the file containing the list of YouTube URLs, formated as .csv", required=True)
    parser.add_argument('--api_key', type=str, help="This is the private key to access the YouTube API", required=True)
    
    parser.add_argument('--url_output',type=str,default='./df_url_description.csv', help="It is the file, formatted as .csv, containing the columns (URL, type, id)")
    parser.add_argument('--video_output',type=str,default='./df_video_metadata.csv', help="It contains the metadata extracted for each unique video_id")    
    parser.add_argument('--channel_output',type=str,default='./df_channel_metadata.csv', help="It contains the metadata extracted for each unique channel")  
    
    parser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
    
    args = parser.parse_args()
    return args

def main():
    args = parse_arguments()
    if args.verbose:
        print("[INFO] - Script started")
        
    # argument parsing
    
    if args.verbose:
        print('[INFO] - ', args)
    
    # input file reading
    df_urls = pd.read_csv(args.input)
    if args.verbose:
        print("[INFO] - Number of URLs in the input file: ", df_urls.shape[0])

    # URLs ids extraction
    df_urls['tuple'] = df_urls.url.apply(lambda url: utils.getIDfromURL(url,YT_API_KEY=args.api_key))
    df_urls[['type','id']] = pd.DataFrame(df_urls['tuple'].tolist(),columns=['type','id'],index=df_urls.index)
    df_urls = df_urls.drop('tuple',axis=1)
    
    if args.verbose:
        print("[INFO] - Number of URLs to process: ", df_urls[(df_urls.type=='video') | (df_urls.type=='channel')].drop_duplicates(subset=['id'],keep='first').shape[0])
        
    df_video_ids = df_urls[df_urls['type']=='video'].drop_duplicates(subset=['id'],keep='first')
    df_channel_ids = df_urls[df_urls['type']=='channel'].drop_duplicates(subset=['id'],keep='first')
    
    if args.verbose:
        print("[INFO] - Number of videos to process: ", df_video_ids.shape[0])
        print("[INFO] - Number of channels to process: ", df_channel_ids.shape[0])

    # metadata retrieval
    yt = YouTubeDataAPI(args.api_key)  
    
    # metadata video
    df_video_metadata = utils.get_video_metadata(yt, df_video_ids.id.tolist(), verbose= args.verbose)
    if args.verbose:
        print("[INFO] - Number of videos whose metadata have been retrieved: ", df_video_metadata.shape[0])
        
    df_video_metadata.to_csv(args.video_output,index=False)
        
    # metadata channel
    if df_channel_ids.shape[0] != 0:
        df_channel_metadata = utils.get_channel_metadata(yt, df_channel_ids.id.tolist(), args.verbose)
        if args.verbose:
            print("[INFO] - Number of channels whose metadata have been retrieved: ", df_channel_metadata.shape[0])    
            df_channel_metadata.to_csv(args.channel_output,index=False)
            ids_exist = df_video_metadata.video_id.tolist() + df_channel_metadata.channel_id.tolist()
            ids_exist = set(ids_exist)
            
            df_urls['status'] = df_urls.id.apply(lambda id: id in ids_exist)
            
            df_urls.loc[df_urls.status==True,'status'] = 'exist'
            df_urls.loc[df_urls.status==False,'status'] = 'moderated'
            df_urls.loc[df_urls.id=='NA','status'] = None
            
            if args.verbose:
                print("[INFO] - Number of moderated resources: ", df_urls[df_urls.status=='moderated'].shape[0])
            
            df_urls.to_csv(args.url_output,index=False)
            
            if args.verbose:
                print("[INFO] - Script ended")
    else:
        if args.verbose:
            print("[INFO] - Number of channels whose metadata have been retrieved: ", 0)
            
            ids_exist = df_video_metadata.video_id.tolist()
            ids_exist = set(ids_exist)
            
            df_urls['status'] = df_urls.id.apply(lambda id: id in ids_exist)
            
            df_urls.loc[df_urls.status==True,'status'] = 'exist'
            df_urls.loc[df_urls.status==False,'status'] = 'moderated'
            df_urls.loc[df_urls.id=='NA','status'] = None
            
            if args.verbose:
                print("[INFO] - Number of moderated resources: ", df_urls[df_urls.status=='moderated'].shape[0])
            
            df_urls.to_csv(args.url_output,index=False)
            
            if args.verbose:
                print("[INFO] - Script ended")
    
  
if __name__ == '__main__':
    main()
