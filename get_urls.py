import pandas as pd
import argparse
bridge_file = open('./Bridge.txt','r')
key_list = bridge_file.readlines()
YT_API_KEY = key_list[0]
#python get_urls.py --input "yt_onlyurl.csv" --inputstart 100000 
def getCIDfromURL(yt_url):
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
        else:
            com_index = yt_url.index('com/')
            if (len(yt_url)-(com_index+4)) == 11:
                link_type = 'video'
                link_id = yt_url[com_index+4:com_index+15]
            else:
                link_type = 'other'
                link_id = 'NA'
    except:
        return 'NA'
    
    return link_id

def getIDfromURL(yt_url):
    link_type = ""
    link_id = 'NA'
    try:
        if '/watch?v=' in yt_url or '/v=' in yt_url or '/shorts/' in yt_url or '.be/' in yt_url:
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
                link_type = 'shorts'
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
        return 'NA'
    return link_id

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, help="This should be the file containing the list of YouTube URLs, formated as .csv", required=True)
    parser.add_argument('--videoflag', type=str, help="Type true if to create a ONLY VIDEOS dataset else for CHANNEL", required=False)
    parser.add_argument('--displayoutput', type=str, help="Just displaying the output stored in df_video_metadata.csv", required=False)    
    parser.add_argument('--inputstart', type=str, help="Start Index for dataset",required=True)
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    if args.displayoutput is "True":
        print('Reading Output file...')
        df_video_metadata = pd.read_csv('./df_video_metadata.csv',low_memory=False)
        print(df_video_metadata)
    else:
        print(f'Now reading {args.input} dataset')
        yt_url = pd.read_csv(f'./Input_Dataset/{args.input}',low_memory=False,lineterminator="\n")
        df_col = list(yt_url.columns)
        if 'url' in df_col and df_col.index('url') != 0:
            print('File Before: ')
            print(yt_url)
            url_column = list(yt_url.pop('url'))
            id_list = []
            for url in url_column:
                url_id = getIDfromURL(url) if args.videoflag == 'True' else getCIDfromURL(url)
                print(url_id)
                if url_id != 'NA':
                    id_list.append(url_id)
            id_list = pd.Series(id_list).value_counts().keys().tolist()
            #print(id_list)
            count = 0
            final_urls = []
            start_index = int(args.inputstart)
            if start_index >= len(id_list):
                print('No Videos left...') if args.videoflag == 'True' else print('No Channels left...')
                return
            for id in id_list[start_index:]:
                final_urls.append(f'https://www.youtube.com/watch?v={id}' if args.videoflag == 'True' else f'https://www.youtube.com/channel/{id}' )
                count = count+1
                if count == 10000:
                    break
            new_url_df = pd.DataFrame({'url': final_urls})
            #yt_url.insert(0, 'url', url_column)
            print('File After: ')
            print(new_url_df)
            if args.videoflag == 'True':
                new_url_df.to_csv(f'./Input_Dataset/{args.input}-modified{args.inputstart}To{(start_index+10000)}.csv',index=False)
                print(f'Dataset is now script compatible, use {args.input}-modified.csv')
                return f'./{args.input}-modified.csv'
            else:
                new_url_df.to_csv(f'./Input_Dataset/{args.input}CH-modified{args.inputstart}To{(start_index+10000)}.csv',index=False)
                print(f'Dataset is now script compatible, use {args.input}CH-modified.csv')
                return f'./{args.input}CH-modified.csv'
        elif df_col.index('url') == 0:
            print('Your dataset is already script compatible')
            return f'./{args.input}.csv'

if __name__ == '__main__':
    main()