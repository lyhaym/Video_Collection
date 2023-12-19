#快手视频下载-最终使用
import os
import re
import requests     # 发送请求
import pprint
import json
import datetime
import time
import requests
from pprint import pprint
import pandas as pd
from pymongo import MongoClient
from fake_useragent import UserAgent

# # 链接数据库
con = MongoClient('127.0.0.1', 27017)
db = con.kuaishou_download
cur = db.down_video0713  # 修改

headers = {
    'Cookie': 'kpf=PC_WEB; clientid=3; did=web_55071bb447589c7864dc89e152acbfc0; userId=3608923495; kuaishou.server.web_st=ChZrdWFpc2hvdS5zZXJ2ZXIud2ViLnN0EqABPM5l2PXKeRZRqcH8dcvEhmw1TuQbq-Jr9cFEZTM2w03Z98V53HBZCx0stKso-jnZxZpZtel4SMMJF03ZVVuu2xI_OiwJKvMWU9qcb7nm_89Ew9TRTUPb8SuRJ2PHmK5i9RMUA9yRQ_a6G29atyVnv6s7mpN8uaZmsOAfTPjCjyfdy418iQcd20vWMaz50HCwaIJCkrD9EDgPLYWWhGbHERoSsmhEcimAl3NtJGybSc8y6sdlIiCj4xoMb_UWXtz1No2avu3grHgKnlR2oQ5SavkN2xgDHSgFMAE; kuaishou.server.web_ph=2126eaf883862122bbc6999656ff16137ec7; kpn=KUAISHOU_VISION',
    #'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    'User-Agent': UserAgent().Chrome  # 谷歌浏览器
}
# url = 'https://www.kuaishou.com/graphql'
url = "https://video.kuaishou.com/graphql"

proxies = {
    'https': '82.157.194.44:7890',
    'http': '82.157.194.44:7890'
}

# 修改
df = pd.read_csv('/Users/lyh/Desktop/AAAAARMBS/Kuaishou/data/xinkuai/down_video/down_video0710.csv', header=0, skiprows = 0)  # 60000
columns_name = ['accountId', 'kuaishouId', 'userid', 'vid_name', 'time', 'indexid']
df.columns = columns_name

for index, row in df.iterrows():
    indexid = row['indexid']
    accountId = row['accountId']
    kuaishouId = row['kuaishouId']
    userid = row['userid']
    vid_name = row['vid_name']  #url="https://www.douyin.com/video/7180961410762001724"
    vid_id = vid_name[vid_name.index("V") + 1:]
    response = requests.get(url=url, headers=headers)
    response.encoding = 'utf-8'
    pprint(response.text)

    json = {"operationName": "visionVideoDetail",
            "variables": {"photoId": vid_id, "page": "selected"},
            "query": "query visionVideoDetail($photoId: String, $type: String, $page: String) {\n  "
                     "visionVideoDetail(photoId: $photoId, type: $type, page: $page) {\n    status\n    type\n    "
                     "author {\n      id\n      name\n      following\n      headerUrl\n      __typename\n    }\n "
                     "   photo {\n      id\n      duration\n      caption\n      likeCount\n      realLikeCount\n "
                     "     coverUrl\n      photoUrl\n      liked\n      timestamp\n      expTag\n      llsid\n    "
                     "  __typename\n    }\n    tags {\n      type\n      name\n      __typename\n    }\n    "
                     "commentLimit {\n      canAddComment\n      __typename\n    }\n    llsid\n    __typename\n  "
                     "}\n}\n "
            }

    # response = requests.post(url=url, headers=headers, proxies=proxies, json=json)
    response = requests.post(url=url, headers=headers, json=json)

    res = response.json()

    print(res)

    try:
        name = res["data"]["visionVideoDetail"]["author"]["name"]
    except Exception as e:
        name = 'None'
        print(e)
    try:
        description = res["data"]["visionVideoDetail"]["photo"]["caption"]
    except Exception as e:
        description = 'None'
        print(e)
    try:
        cover = res["data"]["visionVideoDetail"]["photo"]["coverUrl"]
    except Exception as e:
        cover = 'None'
        print(e)
    try:
        duration = res["data"]["visionVideoDetail"]["photo"]["duration"]
    except Exception as e:
        duration = 'None'
        print(e)
    try:
        like_count = res["data"]["visionVideoDetail"]["photo"]["likeCount"]
    except Exception as e:
        like_count = 'None'
        print(e)
    try:
        video_url = res["data"]["visionVideoDetail"]["photo"]["photoUrl"]
    except Exception as e:
        video_url = 'None'
        print(e)

    info = {
            "author": name,
            "description": description,
            "duration": duration,
            "like_count": like_count,
            "cover": cover,
            "url": video_url,
            'user_id':userid,
            'vid_name': vid_name,
            'vid_id':vid_id,
            'accountId':accountId,
            'kuaishouId':kuaishouId,
            'down_time': datetime.datetime.now()
            }

    cur.insert_one(info)

    # 下载视频
    try:
        video_content = requests.get(url=video_url, headers=headers).content
        # 修改
        out_path = '/Volumes/My Passport/0723Video3/'
        if not os.path.exists(out_path):
            os.makedirs(out_path)
        with open(out_path + vid_name + '.mp4', mode='wb') as f:
            f.write(video_content)


    except Exception as e:
        print(e)
    #time.sleep(5)

    print(indexid, vid_name, "is OK!!!", index)








