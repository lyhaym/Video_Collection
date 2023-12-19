# -*- coding: utf-8 -*-
# one video 最终使用下载版本
import time
import requests
import re
import json
from pprint import pprint
import pandas as pd
from pymongo import MongoClient
#from you_get import common as you_get

# # 链接数据库
con = MongoClient('127.0.0.1', 27017)
db = con.dy_video
cur = db.download

headers={
    'cookie': 'ttwid=1|awcV9QirZUBKt_jepQ2BGwQjqFWSbCrKRqXbJ_NbYKk|1682409721|bb540baa62c6d07475311fbb4d2e6f0a84d98d179417ab0c0e1984b318713652; s_v_web_id=verify_lgwb9a8s_r8fNGcnx_9QNV_45Ly_Bc5r_jDwEwZUsiceu; _tea_utm_cache_2018=undefined; passport_csrf_token=0723bd94d79ad4bfa933903fe71c6cec; passport_csrf_token_default=0723bd94d79ad4bfa933903fe71c6cec; ttcid=38fd676a46234067a9fa50dfc83a852b39; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtY2xpZW50LWNzciI6Ii0tLS0tQkVHSU4gQ0VSVElGSUNBVEUgUkVRVUVTVC0tLS0tXHJcbk1JSUJEakNCdFFJQkFEQW5NUXN3Q1FZRFZRUUdFd0pEVGpFWU1CWUdBMVVFQXd3UFltUmZkR2xqYTJWMFgyZDFcclxuWVhKa01Ga3dFd1lIS29aSXpqMENBUVlJS29aSXpqMERBUWNEUWdBRU1YdkRzLzlUdU5wUHNBNWhxMURObFZPalxyXG55TFRDdG5xZkhFditrTEhmWk9ydUE0K0pCU2VYWlZYdThieERCdmo1QzdPUTNHb3hOTFhOTFFoazV6SHBvS0FzXHJcbk1Db0dDU3FHU0liM0RRRUpEakVkTUJzd0dRWURWUjBSQkJJd0VJSU9kM2QzTG1SdmRYbHBiaTVqYjIwd0NnWUlcclxuS29aSXpqMEVBd0lEU0FBd1JRSWdFa2dlcHdqSkRTSDljYWx5L0owNnNUUS9rWkc5NWhWdjU3NDIwdVFXdDVVQ1xyXG5JUURpQnF5Qk04VVZueG5sNWhPNEJ0dHQyVmZveS9CKysxaVRLWmM0eFpHTkZ3PT1cclxuLS0tLS1FTkQgQ0VSVElGSUNBVEUgUkVRVUVTVC0tLS0tXHJcbiJ9; pwa2="2|0"; douyin.com; csrf_session_id=9939a4adf9f08a01ed329fb6cbbdda8f; download_guide="3/20230425"; VIDEO_FILTER_MEMO_SELECT={"expireTime":1683040094461,"type":1}; strategyABtestKey="1682438631.469"; msToken=uBmv8A39HGWY2Oz_dqjQJL9b7hTl-z80vOJAhpNPlhUhhHFb5dquUg4i3Bx8O02WylwSHrmVoxTfHSia4gOVGP1rcYP4lMAh0VGRVdlRO7aXqBM5lB8Q4d1-EbuvAao0; tt_scid=g.-tJGO0JJ.83h99Sq61PaAF5RIJGkGduYq4oRmD4MUaZTFUfMk1eHj-ORShdJAk0bd8; msToken=xxilrSKQTXN9Bt5Lv3UDgzEwKI3QlvqnH9o5S2LbRS2hAYBrGSwP6cI7I7f1CDA37ogSHDDZng4WdVcPnYUCOynlmPdNTAio8AFjaXyJLZT3Jq_jqVAze27DQFhTZin6; __ac_nonce=0644883af00a1ae828a21; __ac_signature=_02B4Z6wo00f01G6wCgwAAIDDPsdO95xxk.hukA6AAH.sbXE0XDe1htHdjcZ0OPSWN5eRXvut0D343DNRZo87HCh4hPHJ44Jt6.n125g-GK5FAL53l3M00sHdjUyWEayGSyc4912N5G4Sdxyebe; home_can_add_dy_2_desktop="0"',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
}

df = pd.read_excel('/Users/lyh/Desktop/video_url.xlsx', sheet_name='Sheet5', header=0, skiprows = 0)
columns_name = ['doctor_id', 'video_url']
df.columns = columns_name

for index, row in df.iterrows():
    doctor_id = row['doctor_id']
    url = row['video_url']  #url="https://www.douyin.com/video/7180961410762001724"
    response = requests.get(url=url, headers=headers)
    response.encoding = 'utf-8'
    pprint(response.text)

    try:
        title = re.findall('<title data-react-helmet="true">(.*?)</title>',response.text)[0]
    except Exception as e:
        print(e)
        title = -1


    try:
        html_data = re.findall('<script id="RENDER_DATA" type="application/json">(.*?)</script', response.text)[0]
    except Exception as e:
        print(e)
        html_data = -1

    try:
        video_info = requests.utils.unquote(html_data)
    except Exception as e:
        print(e)
        video_info = -1

    try:
        json_data = json.loads(video_info)
    except Exception as e:
        print(e)
        json_data=-1


        #作者信息
    try:
        dy_verify = json_data['41']['aweme']['detail']['authorInfo']['customVerify']
    except Exception as e:
        try:
            dy_verify = json_data['45']['aweme']['detail']['authorInfo']['customVerify']
        except Exception as e:
            print(e)
            dy_verify = -1

    try:
        dy_follower = json_data['41']['aweme']['detail']['authorInfo']['followerCount']
    except Exception as e:
        try:
            dy_follower = json_data['45']['aweme']['detail']['authorInfo']['followerCount']
        except Exception as e:
            print(e)
            dy_follower = -1


    try:
        dy_like = json_data['41']['aweme']['detail']['authorInfo']['totalFavorited']
    except Exception as e:
        try:
            dy_like = json_data['45']['aweme']['detail']['authorInfo']['totalFavorited']
        except Exception as e:
            print(e)
            dy_like = -1


        #单视频页面-视频播放链接
    try:
        video_url = 'https:' + json_data['41']['aweme']['detail']['video']['bitRateList'][0]['playAddr'][0]['src']
    except Exception as e:
        try:
            video_url = 'https:' + json_data['45']['aweme']['detail']['video']['bitRateList'][0]['playAddr'][0]['src']
        except Exception as e:
            print(e)
            video_url = -1

        # 音频下载地址
    try:
        audio_url = 'https:' + json_data['41']['aweme']['detail']['music']['bitRateList'][0]['playAddr'][0][
            'src']
    except Exception as e:
        try:
            audio_url = 'https:' + json_data['45']['aweme']['detail']['music']['playUrl']['uri'][0]
        except Exception as e:
            print(e)
            audio_url = -1


        #视频信息
        #'createTime': 1671947883
    try:
        vid_desc = json_data['41']['aweme']['detail']['desc']
    except Exception as e:
        try:
            vid_desc = json_data['45']['aweme']['detail']['desc']
        except Exception as e:
            print(e)
            vid_desc = -1


    try:
        ocr_content = json_data['41']['aweme']['detail']['seoInfo']['ocrContent']
    except Exception as e:
        try:
            ocr_content = json_data['45']['aweme']['detail']['seoInfo']['ocrContent']
        except Exception as e:
            print(e)
            ocr_content = -1


    try:
        vid_time = json_data['41']['aweme']['detail']['createTime']
        vid_time = time.localtime(vid_time)
        vid_time = time.strftime("%Y-%m-%d %H:%M:%S", vid_time)
    except Exception as e:
        try:
            vid_time = json_data['45']['aweme']['detail']['createTime']
            vid_time = time.localtime(vid_time)
            vid_time = time.strftime("%Y-%m-%d %H:%M:%S", vid_time)
        except Exception as e:
            print(e)
            vid_time = -1


    try:
        vid_collect = json_data['41']['aweme']['detail']['stats']['collectCount']
    except Exception as e:
        try:
            vid_collect = json_data['45']['aweme']['detail']['stats']['collectCount']
        except Exception as e:
            print(e)
            vid_collect = -1


    try:
        vid_comment = json_data['41']['aweme']['detail']['stats']['commentCount']
    except Exception as e:
        try:
            vid_comment = json_data['45']['aweme']['detail']['stats']['commentCount']
        except Exception as e:
            print(e)
            vid_comment = -1


    try:
        vid_like = json_data['41']['aweme']['detail']['stats']['diggCount']
    except Exception as e:
        try:
            vid_like = json_data['45']['aweme']['detail']['stats']['diggCount']
        except Exception as e:
            print(e)
            vid_like = -1


    try:
        vid_share = json_data['41']['aweme']['detail']['stats']['shareCount']
    except Exception as e:
        try:
            vid_share = json_data['45']['aweme']['detail']['stats']['shareCount']
        except Exception as e:
            print(e)
            vid_share = -1


    try:
        vid_id = str(url[29::])     #https://www.douyin.com/video/7180961410762001724
    except Exception as e:
        print(e)
        vid_id = -1

    data = {'doctor_id':doctor_id,'vid_id':str(vid_id),'vid_ids':int(vid_id), 'url':str(url), 'video_url':video_url, 'audio_url':audio_url,
            'vid_time':vid_time, 'vid_desc':vid_desc, 'title':title, 'ocr_content':ocr_content,
                'dy_verify':dy_verify, 'dy_follower':dy_follower, 'dy_like':dy_like,
                'vid_collect':vid_collect,'vid_comment':vid_comment, 'vid_like':vid_like, 'vid_share':vid_share}
    cur.insert_one(data)

    # 下载视频
    try:
        video_content = requests.get(url=video_url, headers=headers).content
        with open('/Users/lyh/Desktop/GD_vid_data/download/' + vid_id + '.mp4', mode='wb') as f:
            f.write(video_content)
    except Exception as e:
        print(e)


    print(index, vid_id, "is OK!!!")

    time.sleep(1)



