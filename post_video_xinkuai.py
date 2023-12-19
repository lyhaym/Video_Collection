import json
import math
import os
import time
from pprint import pprint
import pandas as pd
import requests
import datetime

from pymongo import MongoClient
# # 链接数据库
con = MongoClient('127.0.0.1', 27017)
db = con.kuaishou
cur = db.post_status
cur1 = db.post_list
cur2 = db.post_topic
cur3 = db.post_cmt
cur4 = db.post_agg


def get_photolist(accountId, photolisturl, startTime, endTime):
    # payloadData数据
    payloadData = {"day": "",
                   "startTime": startTime,
                   "endTime": endTime,
                   "accountId": accountId,
                   "keyword": "",
                   "isHotAwene": "",
                   "isProduct": "",
                   "isTags": "",
                   "start": 1,
                   "size": 100,
                   "sort": "viewCount"}

    # 页面响应
    r = requests.post(photolisturl, data=json.dumps(payloadData), headers=payloadHeader)
    dumpJsonData = json.dumps(payloadData)
    print(f"dumpJsonData = {dumpJsonData}")
    res = requests.post(photolisturl, data=dumpJsonData, headers=payloadHeader,
                        allow_redirects=True)  # 下面这种直接填充json参数的方式也OK # # 下载超时 timeOut = 25 # res = requests.post(postUrl, json=payloadData, headers=header)
    print(f"responseTime = {datetime.datetime.now()}, statusCode = {res.status_code}, res text = {res.text}")
    json_data = json.loads(res.text)  #
    pprint(json_data)

    try:
        anaTags = []
        for item in json_data['data']['list']:
            anaTags.append(item['anaTags'])
        # print(anaTags)
    except Exception as e:
        print(e)
        anaTags = []

    try:
        caption = []
        for item in json_data['data']['list']:
            caption.append(item['caption'])
    except Exception as e:
        print(e)
        caption = []

    try:
        collectCount = []
        for item in json_data['data']['list']:
            collectCount.append(item['collectCount'])
    except Exception as e:
        print(e)
        collectCount = []

    try:
        commentCount = []
        for item in json_data['data']['list']:
            commentCount.append(item['commentCount'])
    except Exception as e:
        print(e)
        commentCount = []

    try:
        cover = []
        for item in json_data['data']['list']:
            cover.append(item['cover'])
    except Exception as e:
        print(e)
        cover = []

    try:
        headurls = []
        for item in json_data['data']['list']:
            headurls.append(item['headurls'])
    except Exception as e:
        print(e)
        headurls = []

    try:
        isPromotion = []
        for item in json_data['data']['list']:
            isPromotion.append(item['isPromotion'])
    except Exception as e:
        print(e)
        isPromotion = []

    try:
        likeCount = []
        for item in json_data['data']['list']:
            likeCount.append(item['likeCount'])
    except Exception as e:
        print(e)
        likeCount = []

    try:
        mainMvUrl = []
        for item in json_data['data']['list']:
            mainMvUrl.append(item['mainMvUrl'])
    except Exception as e:
        print(e)
        mainMvUrl = []

    try:
        photoId = []
        for item in json_data['data']['list']:
            photoid = 'V' + item['photoId']
            photoId.append(photoid)
    except Exception as e:
        print(e)
        photoId = []

    try:
        shareCount = []
        for item in json_data['data']['list']:
            shareCount.append(item['shareCount'])
    except Exception as e:
        print(e)
        shareCount = []

    try:
        time = []
        for item in json_data['data']['list']:
            time.append(item['time'])
    except Exception as e:
        print(e)
        time = []

    try:
        updateTime = []
        for item in json_data['data']['list']:
            updateTime.append(item['updateTime'])
    except Exception as e:
        print(e)
        updateTime = []

    try:
        userId = []
        for item in json_data['data']['list']:
            userId.append(item['userId'])
    except Exception as e:
        print(e)
        userId = []

    try:
        userName = []
        for item in json_data['data']['list']:
            userName.append(item['userName'])
    except Exception as e:
        print(e)
        userName = []

    try:
        viewCount = []
        for item in json_data['data']['list']:
            viewCount.append(item['viewCount'])
    except Exception as e:
        print(e)
        viewCount = []

    try:
        completionRate = []
        for item in json_data['data']['list']:
            completionRate.append(item['completionRate'])
    except Exception as e:
        print(e)
        completionRate = []



    PhotoList = pd.DataFrame({'accountId': accountId, 'kuaishouId': kuaishouId, 'userid': userid, 'index':index, 'photoId': photoId,
                              'anaTags': anaTags, 'caption': caption, 'completionRate': completionRate, 'collectCount': collectCount,
                              'commentCount': commentCount, 'headurls': headurls, 'cover': cover,
                              'isPromotion': isPromotion, 'likeCount': likeCount, 'mainMvUrl': mainMvUrl,
                              'shareCount': shareCount, 'viewCount': viewCount,
                              'userId': userId, 'userName': userName, 'time': time,
                              'updateTime': updateTime, 'get_time': datetime.datetime.now()})
    print('PhotoList OK')

    data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'userid': userid, 'index': index, 'photoId': photoId,
                                'anaTags': anaTags, 'caption': caption, 'completionRate': completionRate, 'collectCount': collectCount,
                                'commentCount': commentCount, 'headurls': headurls, 'cover': cover,
                                'isPromotion': isPromotion, 'likeCount': likeCount, 'mainMvUrl': mainMvUrl,
                                'shareCount': shareCount, 'viewCount': viewCount,
                                'userId': userId, 'userName': userName, 'time': time,
                                'updateTime': updateTime, 'get_time': datetime.datetime.now()}
    cur1.insert_one(data)

    csv_path = os.path.join(folder_path, str(accountId) + "_PhotoList.csv")
    PhotoList.to_csv(csv_path, index=False)

    print('PhotoList', index, accountId, userid)

    return res.text

def get_photolists(accountId, photolisturl, i, startTime, endTime):
    # payloadData数据
    payloadData = {"day": "", # 7
                   "startTime": startTime,
                   "endTime": endTime,
                   "accountId": accountId,
                   "keyword": "",
                   "isHotAwene": "",
                   "isProduct": "",
                   "isTags": "",
                   "start": i,
                   "size": 100,
                   "sort": "viewCount"}

    # 页面响应
    r = requests.post(photolisturl, data=json.dumps(payloadData), headers=payloadHeader)
    dumpJsonData = json.dumps(payloadData)
    # print(f"dumpJsonData = {dumpJsonData}")
    res = requests.post(photolisturl, data=dumpJsonData, headers=payloadHeader, allow_redirects=True) # 下面这种直接填充json参数的方式也OK # # 下载超时 timeOut = 25 # res = requests.post(postUrl, json=payloadData, headers=header)
    print(f"responseTime = {datetime.datetime.now()}, statusCode = {res.status_code}) # , res text = {res.text}")
    json_data = json.loads(res.text)  #
    #pprint(json_data)

    try:
        anaTags = []
        for item in json_data['data']['list']:
            anaTags.append(item['anaTags'])
        # print(anaTags)
    except Exception as e:
        print(e)
        anaTags = []

    try:
        caption = []
        for item in json_data['data']['list']:
            caption.append(item['caption'])
    except Exception as e:
        print(e)
        caption = []

    try:
        collectCount = []
        for item in json_data['data']['list']:
            collectCount.append(item['collectCount'])
    except Exception as e:
        print(e)
        collectCount = []

    try:
        commentCount = []
        for item in json_data['data']['list']:
            commentCount.append(item['commentCount'])
    except Exception as e:
        print(e)
        commentCount = []

    try:
        cover = []
        for item in json_data['data']['list']:
            cover.append(item['cover'])
    except Exception as e:
        print(e)
        cover = []

    try:
        headurls = []
        for item in json_data['data']['list']:
            headurls.append(item['headurls'])
    except Exception as e:
        print(e)
        headurls = []

    try:
        isPromotion = []
        for item in json_data['data']['list']:
            isPromotion.append(item['isPromotion'])
    except Exception as e:
        print(e)
        isPromotion = []

    try:
        likeCount = []
        for item in json_data['data']['list']:
            likeCount.append(item['likeCount'])
    except Exception as e:
        print(e)
        likeCount = []

    try:
        mainMvUrl = []
        for item in json_data['data']['list']:
            mainMvUrl.append(item['mainMvUrl'])
    except Exception as e:
        print(e)
        mainMvUrl = []

    try:
        photoId = []
        for item in json_data['data']['list']:
            photoid = 'V' + item['photoId']
            photoId.append(photoid)
    except Exception as e:
        print(e)
        photoId = []

    try:
        shareCount = []
        for item in json_data['data']['list']:
            shareCount.append(item['shareCount'])
    except Exception as e:
        print(e)
        shareCount = []

    try:
        time = []
        for item in json_data['data']['list']:
            time.append(item['time'])
    except Exception as e:
        print(e)
        time = []

    try:
        updateTime = []
        for item in json_data['data']['list']:
            updateTime.append(item['updateTime'])
    except Exception as e:
        print(e)
        updateTime = []

    try:
        userId = []
        for item in json_data['data']['list']:
            userId.append(item['userId'])
    except Exception as e:
        print(e)
        userId = []

    try:
        userName = []
        for item in json_data['data']['list']:
            userName.append(item['userName'])
    except Exception as e:
        print(e)
        userName = []

    try:
        viewCount = []
        for item in json_data['data']['list']:
            viewCount.append(item['viewCount'])
    except Exception as e:
        print(e)
        viewCount = []

    try:
        completionRate = []
        for item in json_data['data']['list']:
            completionRate.append(item['completionRate'])
    except Exception as e:
        print(e)
        completionRate = []


    PhotoList = pd.DataFrame({'accountId': accountId, 'kuaishouId': kuaishouId, 'userid': userid, 'index':index, 'photoId': photoId,
                                'anaTags': anaTags, 'caption': caption, 'completionRate': completionRate, 'collectCount': collectCount,
                                'commentCount': commentCount, 'headurls': headurls, 'cover': cover,
                                'isPromotion': isPromotion, 'likeCount': likeCount, 'mainMvUrl': mainMvUrl,
                                'shareCount': shareCount, 'viewCount': viewCount,
                                'userId': userId, 'userName': userName, 'time': time,
                                'updateTime': updateTime, 'get_time': datetime.datetime.now()})

    data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'userid': userid, 'index':index, 'photoId': photoId,
                                'anaTags': anaTags, 'caption': caption, 'completionRate': completionRate, 'collectCount': collectCount,
                                'commentCount': commentCount, 'headurls': headurls, 'cover': cover,
                                'isPromotion': isPromotion, 'likeCount': likeCount, 'mainMvUrl': mainMvUrl,
                                'shareCount': shareCount, 'viewCount': viewCount,
                                'userId': userId, 'userName': userName, 'time': time,
                                'updateTime': updateTime, 'get_time': datetime.datetime.now()}
    cur1.insert_one(data)

    csv_path = os.path.join(folder_path, str(accountId) + "_PhotoList" + str(i) + ".csv")
    PhotoList.to_csv(csv_path, index=False)

    print('PhotoList', index, accountId,userid)

    return res.text

def get_phototopic(accountId, phototopicyrl, startTime, endTime):
    # payloadData数据
    payloadData = {"day": "",
                   "startTime": startTime,
                   "endTime": endTime,
                   "accountId": accountId}

    # 页面响应
    r = requests.post(phototopicyrl, data=json.dumps(payloadData), headers=payloadHeader)
    dumpJsonData = json.dumps(payloadData)
    #print(f"dumpJsonData = {dumpJsonData}")
    res = requests.post(phototopicyrl, data=dumpJsonData, headers=payloadHeader,
                        allow_redirects=True)  # 下面这种直接填充json参数的方式也OK # # 下载超时 timeOut = 25 # res = requests.post(postUrl, json=payloadData, headers=header)
    print(f"responseTime = {datetime.datetime.now()}, statusCode = {res.status_code}, res text = {res.text}")
    json_data = json.loads(res.text)  #
    #pprint(json_data)

    try:
        collectCount = []
        for item in json_data['data']:
            collectCount.append(item['collectCount'])
        # print(collectCount)
    except Exception as e:
        print(e)
        collectCount = []

    try:
        commentCount = []
        for item in json_data['data']:
            commentCount.append(item['commentCount'])
    except Exception as e:
        print(e)
        commentCount = []

    try:
        count = []
        for item in json_data['data']:
            count.append(item['count'])
    except Exception as e:
        print(e)
        count = []

    try:
        lastUpdateTime = []
        for item in json_data['data']:
            lastUpdateTime.append(item['lastUpdateTime'])
    except Exception as e:
        print(e)
        lastUpdateTime = []

    try:
        likeCount = []
        for item in json_data['data']:
            likeCount.append(item['likeCount'])
    except Exception as e:
        print(e)
        likeCount = []

    try:
        shareCount = []
        for item in json_data['data']:
            shareCount.append(item['shareCount'])
    except Exception as e:
        print(e)
        shareCount = []

    try:
        topic = []
        for item in json_data['data']:
            topic.append(item['topic'])
    except Exception as e:
        print(e)
        topic = []

    try:
        viewCount = []
        for item in json_data['data']:
            viewCount.append(item['viewCount'])
    except Exception as e:
        print(e)
        viewCount = []

    TopicList = pd.DataFrame({'accountId': accountId, 'kuaishouId': kuaishouId, 'userid': userid, 'index':index,
                              'collectCount': collectCount,
                              'commentCount': commentCount, 'count': count, 'lastUpdateTime': lastUpdateTime,
                              'likeCount': likeCount, 'shareCount': shareCount,
                              'topic': topic, 'viewCount': viewCount,
                              'get_time': datetime.datetime.now()})

    data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index':index, 'userid': userid,
                              'collectCount': collectCount,
                              'commentCount': commentCount, 'count': count, 'lastUpdateTime': lastUpdateTime,
                              'likeCount': likeCount, 'shareCount': shareCount,
                              'topic': topic, 'viewCount': viewCount,
                              'get_time': datetime.datetime.now()}
    cur2.insert_one(data)

    csv_path = os.path.join(folder_path, str(accountId) + "_TopicList.csv")
    TopicList.to_csv(csv_path, index=False)

    print('TopicList', index, accountId,userid)

    return res.text

def get_photocmt(accountId, cmturl, startTime, endTime):
    # payloadData数据
    payloadData = {"day": "",
                   "startTime": startTime,
                   "endTime": endTime,
                   "accountId": accountId,
                   "keyword": "",
                   "start": 1,
                   "size": 100}  # 最大还是100

    # 页面响应
    r = requests.post(cmturl, data=json.dumps(payloadData), headers=payloadHeader)
    dumpJsonData = json.dumps(payloadData)
    #print(f"dumpJsonData = {dumpJsonData}")
    res = requests.post(cmturl, data=dumpJsonData, headers=payloadHeader,
                        allow_redirects=True)  # 下面这种直接填充json参数的方式也OK # # 下载超时 timeOut = 25 # res = requests.post(postUrl, json=payloadData, headers=header)
    print(f"responseTime = {datetime.datetime.now()}, statusCode = {res.status_code}, res text = {res.text}")
    json_data = json.loads(res.text)  #
    #pprint(json_data)

    try:
        content = []
        for item in json_data['data']['list']:
            content.append(item['content'])
        # print(content)
    except Exception as e:
        print(e)
        content = []

    try:
        headurl = []
        for item in json_data['data']['list']:
            headurl.append(item['headurl'])
    except Exception as e:
        print(e)
        headurl = []

    try:
        time = []
        for item in json_data['data']['list']:
            time.append(item['time'])
    except Exception as e:
        print(e)
        time = []

    try:
        userId = []
        for item in json_data['data']['list']:
            userId.append(item['userId'])
    except Exception as e:
        print(e)
        userId = []

    CmtList = pd.DataFrame({'accountId': accountId, 'kuaishouId': kuaishouId, 'index':index, 'userid': userid,
                            'content': content,
                            'headurl': headurl, 'time': time, 'userId': userId,
                            'get_time': datetime.datetime.now()})

    data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index':index, 'userid': userid,
                            'content': content,
                            'headurl': headurl, 'time': time, 'userId': userId,
                            'get_time': datetime.datetime.now()}
    cur3.insert_one(data)

    csv_path = os.path.join(folder_path, str(accountId) + "_CmtList.csv")
    CmtList.to_csv(csv_path, index=False)

    print('CmtList', index, accountId,userid)

    return res.text

def get_photoagg(accountId, aggurl, startTime, endTime):
    # payloadData数据
    payloadData = {"day": "",
                   "startTime": startTime,
                   "endTime": endTime,
                   "accountId": accountId}

    # 页面响应
    r = requests.post(aggurl, data=json.dumps(payloadData), headers=payloadHeader)
    dumpJsonData = json.dumps(payloadData)
    #print(f"dumpJsonData = {dumpJsonData}")
    res = requests.post(aggurl, data=dumpJsonData, headers=payloadHeader,
                        allow_redirects=True)  # 下面这种直接填充json参数的方式也OK # # 下载超时 timeOut = 25 # res = requests.post(postUrl, json=payloadData, headers=header)
    print(f"responseTime = {datetime.datetime.now()}, statusCode = {res.status_code}, res text = {res.text}")
    json_data = json.loads(res.text)  #
    #pprint(json_data)

    try:
        avgCollectCount = json_data['data']['avgCollectCount']
    except Exception as e:
        avgCollectCount = 'wrong'
        print(e)

    try:
        avgCommentCount = json_data['data']['avgCommentCount']
    except Exception as e:
        avgCommentCount = 'wrong'
        print(e)

    try:
        avgLikeCount = json_data['data']['avgLikeCount']
    except Exception as e:
        avgLikeCount = 'wrong'
        print(e)

    try:
        avgLikeShareRate = json_data['data']['avgLikeShareRate']
    except Exception as e:
        avgLikeShareRate = 'wrong'
        print(e)

    try:
        avgShareCount = json_data['data']['avgShareCount']
    except Exception as e:
        avgShareCount = 'wrong'
        print(e)

    try:
        avgViewCount = json_data['data']['avgViewCount']
    except Exception as e:
        avgViewCount = 'wrong'
        print(e)

    try:
        collectCount = json_data['data']['collectCount']
    except Exception as e:
        collectCount = 'wrong'
        print(e)

    try:
        collectCountRate = json_data['data']['collectCountRate']
    except Exception as e:
        collectCountRate = 'wrong'
        print(e)

    try:
        commentCount = json_data['data']['commentCount']
    except Exception as e:
        commentCount = 'wrong'
        print(e)

    try:
        commentCountRate = json_data['data']['commentCountRate']
    except Exception as e:
        commentCountRate = 'wrong'
        print(e)

    try:
        hotAweneRatio = json_data['data']['hotAweneRatio']
    except Exception as e:
        hotAweneRatio = 'wrong'
        print(e)

    try:
        likeCount = json_data['data']['likeCount']
    except Exception as e:
        likeCount = 'wrong'
        print(e)

    try:
        likeCountRate = json_data['data']['likeCountRate']
    except Exception as e:
        likeCountRate = 'wrong'
        print(e)

    try:
        middleCollectCount = json_data['data']['middleCollectCount']
    except Exception as e:
        middleCollectCount = 'wrong'
        print(e)

    try:
        middleCommentCount = json_data['data']['middleCommentCount']
    except Exception as e:
        middleCommentCount = 'wrong'
        print(e)

    try:
        middleLikeCount = json_data['data']['middleLikeCount']
    except Exception as e:
        middleLikeCount = 'wrong'
        print(e)

    try:
        middleShareCount = json_data['data']['middleShareCount']
    except Exception as e:
        middleShareCount = 'wrong'
        print(e)

    try:
        middleViewCount = json_data['data']['middleViewCount']
    except Exception as e:
        middleViewCount = 'wrong'
        print(e)

    try:
        photoCount = json_data['data']['photoCount']
    except Exception as e:
        photoCount = 0
        print(e)

    try:
        photoCountRate = json_data['data']['photoCountRate']
    except Exception as e:
        photoCountRate = 'wrong'
        print(e)

    try:
        shareCount = json_data['data']['shareCount']
    except Exception as e:
        shareCount = 'wrong'
        print(e)

    try:
        shareCountRate = json_data['data']['shareCountRate']
    except Exception as e:
        shareCountRate = 'wrong'
        print(e)

    try:
        updateStatus = json_data['data']['updateStatus']
    except Exception as e:
        updateStatus = 'wrong'
        print(e)

    try:
        viewCount = json_data['data']['viewCount']
    except Exception as e:
        viewCount = 'wrong'
        print(e)

    try:
        viewCountRate = json_data['data']['viewCountRate']
    except Exception as e:
        viewCountRate = 'wrong'
        print(e)

    AggList = pd.DataFrame({'accountId': accountId, 'kuaishouId': kuaishouId, 'index':index, 'userid': userid,
                            'avgCollectCount': avgCollectCount,'avgCommentCount': avgCommentCount,
                            'avgLikeCount': avgLikeCount, 'avgLikeShareRate': avgLikeShareRate,
                            'avgShareCount': avgShareCount, 'avgViewCount': avgViewCount,
                            'collectCount': collectCount, 'collectCountRate': collectCountRate,
                            'commentCount': commentCount, 'commentCountRate': commentCountRate,
                            'hotAweneRatio': hotAweneRatio, 'likeCount': likeCount,
                            'likeCountRate': likeCountRate, 'middleCollectCount': middleCollectCount,
                            'middleCommentCount': middleCommentCount, 'middleLikeCount': middleLikeCount,
                            'middleShareCount': middleShareCount, 'middleViewCount': middleViewCount,
                            'photoCount': photoCount, 'photoCountRate': photoCountRate,
                            'shareCount': shareCount, 'shareCountRate': shareCountRate,
                            'updateStatus': updateStatus, 'viewCount': viewCount,
                            'viewCountRate': viewCountRate,
                            'get_time': datetime.datetime.now()}, index=[0])


    csv_path = os.path.join(folder_path, str(accountId) + "_AggList.csv")
    AggList.to_csv(csv_path, index=False)

    data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index':index, 'userid': userid,
                                'avgCollectCount': avgCollectCount, 'avgCommentCount': avgCommentCount,
                                'avgLikeCount': avgLikeCount, 'avgLikeShareRate': avgLikeShareRate,
                                'avgShareCount': avgShareCount, 'avgViewCount': avgViewCount,
                                'collectCount': collectCount, 'collectCountRate': collectCountRate,
                                'commentCount': commentCount, 'commentCountRate': commentCountRate,
                                'hotAweneRatio': hotAweneRatio, 'likeCount': likeCount,
                                'likeCountRate': likeCountRate, 'middleCollectCount': middleCollectCount,
                                'middleCommentCount': middleCommentCount, 'middleLikeCount': middleLikeCount,
                                'middleShareCount': middleShareCount, 'middleViewCount': middleViewCount,
                                'photoCount': photoCount, 'photoCountRate': photoCountRate,
                                'shareCount': shareCount, 'shareCountRate': shareCountRate,
                                'updateStatus': updateStatus, 'viewCount': viewCount, 'viewCountRate': viewCountRate,
                                'get_time': datetime.datetime.now()}
    cur4.insert_one(data)

    print('AggList', index, accountId,userid)

    return photoCount


def get_accountagg(accountId, accounturl, startTime, endTime):
    # payloadData数据
    payloadData = {"day": "",
                   "startTime": startTime,
                   "endTime": endTime,
                   "accountId": accountId}

    # 页面响应
    r = requests.post(accounturl, data=json.dumps(payloadData), headers=payloadHeader)
    dumpJsonData = json.dumps(payloadData)
    #print(f"dumpJsonData = {dumpJsonData}")
    res = requests.post(accounturl, data=dumpJsonData, headers=payloadHeader,
                        allow_redirects=True)  # 下面这种直接填充json参数的方式也OK # # 下载超时 timeOut = 25 # res = requests.post(postUrl, json=payloadData, headers=header)
    print(f"responseTime = {datetime.datetime.now()}, statusCode = {res.status_code}, res text = {res.text}")
    json_data = json.loads(res.text)  #
    #pprint(json_data)

    try:
        merchant = json_data['data']['merchant']
    except Exception as e:
        merchant = 'wrong'
        print(e)

    try:
        photoCount = json_data['data']['photoCount']
    except Exception as e:
        photoCount = 'wrong'
        print(e)

    try:
        photoCountRatio = json_data['data']['photoCountRatio']
    except Exception as e:
        photoCountRatio = 'wrong'
        print(e)

    try:
        avgInteraction = json_data['data']['avgInteraction']
    except Exception as e:
        avgInteraction = 'wrong'
        print(e)

    try:
        avgInteractionRatio = json_data['data']['avgInteractionRatio']
    except Exception as e:
        avgInteractionRatio = 'wrong'
        print(e)

    try:
        avgProductCount = json_data['data']['avgProductCount']
    except Exception as e:
        avgProductCount = 'wrong'
        print(e)

    try:
        avgSales = json_data['data']['avgSales']
    except Exception as e:
        avgSales = 'wrong'
        print(e)

    try:
        avgSalesConversion = json_data['data']['avgSalesConversion']
    except Exception as e:
        avgSalesConversion = 'wrong'
        print(e)

    try:
        avgSalesMoney = json_data['data']['avgSalesMoney']
    except Exception as e:
        avgSalesMoney = 'wrong'
        print(e)

    try:
        avgWatchCount = json_data['data']['avgWatchCount']
    except Exception as e:
        avgWatchCount = 'wrong'
        print(e)

    try:
        avgWatchCountRatio = json_data['data']['avgWatchCountRatio']
    except Exception as e:
        avgWatchCountRatio = 'wrong'
        print(e)

    try:
        awemeCountAvg = json_data['data']['awemeCountAvg']
    except Exception as e:
        awemeCountAvg = 'wrong'
        print(e)

    try:
        fansAdd = json_data['data']['fansAdd']
    except Exception as e:
        fansAdd = 'wrong'
        print(e)

    try:
        fansAddRatio = json_data['data']['fansAddRatio']
    except Exception as e:
        fansAddRatio = 'wrong'
        print(e)

    try:
        followerAddRankRate = json_data['data']['followerAddRankRate']
    except Exception as e:
        followerAddRankRate = 'wrong'
        print(e)

    try:
        hotAwemeCountRankRate = json_data['data']['hotAwemeCountRankRate']
    except Exception as e:
        hotAwemeCountRankRate = 'wrong'
        print(e)

    try:
        hotAwene = json_data['data']['hotAwene']
    except Exception as e:
        hotAwene = 'wrong'
        print(e)

    try:
        hotAweneRatio = json_data['data']['hotAweneRatio']
    except Exception as e:
        hotAweneRatio = 'wrong'
        print(e)

    try:
        interactionAvgRankRate = json_data['data']['interactionAvgRankRate']
    except Exception as e:
        interactionAvgRankRate = 'wrong'
        print(e)

    try:
        interactionMedianNum = json_data['data']['interactionMedianNum']
    except Exception as e:
        interactionMedianNum = 'wrong'
        print(e)

    try:
        interactionMedianNumRate = json_data['data']['interactionMedianNumRate']
    except Exception as e:
        interactionMedianNumRate = 'wrong'
        print(e)

    try:
        interactionMedianRankRate = json_data['data']['interactionMedianRankRate']
    except Exception as e:
        interactionMedianRankRate = 'wrong'
        print(e)

    try:
        liveCount = json_data['data']['liveCount']
    except Exception as e:
        liveCount = 'wrong'
        print(e)

    try:
        liveCountRankRate = json_data['data']['liveCountRankRate']
    except Exception as e:
        liveCountRankRate = 'wrong'
        print(e)

    try:
        liveCountRatio = json_data['data']['liveCountRatio']
    except Exception as e:
        liveCountRatio = 'wrong'
        print(e)

    try:
        liveGoodsCountRankRate = json_data['data']['liveGoodsCountRankRate']
    except Exception as e:
        liveGoodsCountRankRate = 'wrong'
        print(e)

    try:
        liveMoneyRankRate = json_data['data']['liveMoneyRankRate']
    except Exception as e:
        liveMoneyRankRate = 'wrong'
        print(e)

    try:
        liveTransformRankRate = json_data['data']['liveTransformRankRate']
    except Exception as e:
        liveTransformRankRate = 'wrong'
        print(e)

    try:
        liveVolumeRankRate = json_data['data']['liveVolumeRankRate']
    except Exception as e:
        liveVolumeRankRate = 'wrong'
        print(e)

    try:
        liveWithProductCount = json_data['data']['liveWithProductCount']
    except Exception as e:
        liveWithProductCount = 'wrong'
        print(e)

    try:
        watchAvgRankRate = json_data['data']['watchAvgRankRate']
    except Exception as e:
        watchAvgRankRate = 'wrong'
        print(e)

    accountOverview = pd.DataFrame({'accountId': accountId, 'kuaishouId': kuaishouId, 'index':index, 'userid': userid,
                            'merchant': merchant,'photoCount': photoCount,
                            'photoCountRatio': photoCountRatio, 'avgInteraction': avgInteraction,
                            'avgInteractionRatio': avgInteractionRatio, 'avgProductCount': avgProductCount,
                            'avgSales': avgSales, 'avgSalesConversion': avgSalesConversion,
                            'avgSalesMoney': avgSalesMoney, 'avgWatchCount': avgWatchCount,
                            'hotAweneRatio': hotAweneRatio, 'avgWatchCountRatio': avgWatchCountRatio,
                            'awemeCountAvg': awemeCountAvg, 'fansAdd': fansAdd,
                            'fansAddRatio': fansAddRatio, 'followerAddRankRate': followerAddRankRate,
                            'hotAwemeCountRankRate': hotAwemeCountRankRate, 'hotAwene': hotAwene,
                            'watchAvgRankRate': watchAvgRankRate, 'interactionAvgRankRate': interactionAvgRankRate,
                            'interactionMedianNum': interactionMedianNum, 'interactionMedianNumRate': interactionMedianNumRate,
                            'interactionMedianRankRate': interactionMedianRankRate, 'liveCount': liveCount,
                            'liveCountRankRate': liveCountRankRate, 'liveCountRatio': liveCountRatio,
                            'liveGoodsCountRankRate': liveGoodsCountRankRate, 'liveMoneyRankRate': liveMoneyRankRate,
                            'liveTransformRankRate': liveTransformRankRate, 'liveVolumeRankRate': liveVolumeRankRate,
                            'liveWithProductCount': liveWithProductCount,
                            'get_time': datetime.datetime.now()}, index=[0])


    csv_path = os.path.join(folder_path, str(accountId) + "_accountOverview.csv")
    accountOverview.to_csv(csv_path, index=False)

    data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index':index, 'userid': userid,
                            'merchant': merchant,'photoCount': photoCount,
                            'photoCountRatio': photoCountRatio, 'avgInteraction': avgInteraction,
                            'avgInteractionRatio': avgInteractionRatio, 'avgProductCount': avgProductCount,
                            'avgSales': avgSales, 'avgSalesConversion': avgSalesConversion,
                            'avgSalesMoney': avgSalesMoney, 'avgWatchCount': avgWatchCount,
                            'hotAweneRatio': hotAweneRatio, 'avgWatchCountRatio': avgWatchCountRatio,
                            'awemeCountAvg': awemeCountAvg, 'fansAdd': fansAdd,
                            'fansAddRatio': fansAddRatio, 'followerAddRankRate': followerAddRankRate,
                            'hotAwemeCountRankRate': hotAwemeCountRankRate, 'hotAwene': hotAwene,
                            'watchAvgRankRate': watchAvgRankRate, 'interactionAvgRankRate': interactionAvgRankRate,
                            'interactionMedianNum': interactionMedianNum, 'interactionMedianNumRate': interactionMedianNumRate,
                            'interactionMedianRankRate': interactionMedianRankRate, 'liveCount': liveCount,
                            'liveCountRankRate': liveCountRankRate, 'liveCountRatio': liveCountRatio,
                            'liveGoodsCountRankRate': liveGoodsCountRankRate, 'liveMoneyRankRate': liveMoneyRankRate,
                            'liveTransformRankRate': liveTransformRankRate, 'liveVolumeRankRate': liveVolumeRankRate,
                            'liveWithProductCount': liveWithProductCount,
                            'get_time': datetime.datetime.now()}
    cur4.insert_one(data)

    print('accountOverview', index, accountId)

    return res.text


if __name__ == '__main__':

    # 网址
    # 视频列表/每周
    photolisturl = 'https://gw.newrank.cn/api/xk/xdnphb/nr/cloud/ks/account/accountDetailPhotoAnalysisList'
    # 视频对应话题/每周
    phototopicyrl = 'https://gw.newrank.cn/api/xk/xdnphb/nr/cloud/ks/account/accountDetailPhotoAnalysisTopic'
    # 视频对应评论/每周主要
    cmturl = 'https://gw.newrank.cn/api/xk/xdnphb/nr/cloud/ks/account/accountDetailPhotoAnalysisComment'
    # 视频汇总/每周
    aggurl = 'https://gw.newrank.cn/api/xk/xdnphb/nr/cloud/ks/account/accountDetailPhotoAnalysisAggData'
    # accounturl = 'https://gw.newrank.cn/api/xk/xdnphb/nr/cloud/ks/account/accountDetailOverViewDataPerformance'

    # 请求设置 修改
    payloadHeader = {
        'Origin': 'https://xk.newrank.cn',
        'Connection': 'keep-alive',
        'Referer': 'https://xk.newrank.cn/',
        'Host': 'gw.newrank.cn',
        'N-Token': '96a7725673b545599bb1ec18898ad603',
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': 'Hm_lvt_a19fd7224d30e3c8a6558dcb38c4beed=1695219658; Hm_lpvt_a19fd7224d30e3c8a6558dcb38c4beed=1695284865; amp_6e403e=hi5IWT4x_GI6BLvJYmFlbg...1hardks3i.1hardks3i.0.0.0; __root_domain_v=.newrank.cn; _qddaz=QD.962095980019803; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22nr_b9bag954g%22%2C%22first_id%22%3A%2218ae012befcc8e-09e96289843fc58-19525634-1484784-18ae012befd12b4%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%2218ae012befcc8e-09e96289843fc58-19525634-1484784-18ae012befd12b4%22%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMThiNGM0OWU4YzVjMDgtMDUyODQyZGNhYzM2Mzg0LTE5NTI1NjM0LTE0ODQ3ODQtMThiNGM0OWU4YzYyYjFjIiwiJGlkZW50aXR5X2xvZ2luX2lkIjoibnJfYjliYWc5NTRnIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22nr_b9bag954g%22%7D%7D; acw_tc=707c9fc716977919954052985e44f1fa4816dd2c47a0f0f0ba3dd4a6589f54; token=DF6BFA623AF240C0995FBEDC8B21A33A',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
    }

# 循环获得用户id 修改
    df = pd.read_csv('/Users/lyh/Desktop/AAAAARMBS/Kuaishou/data/accountid.csv', dtype=object, header=0, skiprows = 0)
    # df = pd.read_csv('/Users/lyh/Desktop/AAAAARMBS/Kuaishou/data/other_accountid.csv', header=0, dtype=object, skiprows = 1519-1519)
    columns_name = ['accountId', 'kuaishouId', 'userid']
    df.columns = columns_name
    x = 0   # 907

    for index, row in df.iterrows():
        userid = int(row['userid'])

        if userid < 2520:  # 修改

            accountId = row['accountId']
            kuaishouId = row['kuaishouId']

            # 时间范围 修改
            startTime = '2023-04-24'
            endTime = '2023-05-01'  # 采集数据的前一天 9-15 是7天的数据

            # 输出路径 修改
            folder_path = "/Users/lyh/Desktop/AAAAARMBS/Kuaishou/data/xinkuai/0501_other/" + str(accountId)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # 采集数据
            # return5 = get_accountagg(accountId, accounturl, startTime, endTime) # 无法自定义时间 在收藏夹导出
            # time.sleep(5)
            photoCount = get_photoagg(accountId, aggurl, startTime, endTime)
            x = x + 1
            time.sleep(2)
            photoCount = int(photoCount)

            if photoCount == 0:

                # 无作品列表
                PhotoList = pd.DataFrame(
                    {'accountId': accountId, 'kuaishouId': kuaishouId, 'userid': userid, 'index': index, 'photoId': 'NA',
                     'anaTags': 'NA', 'caption': 'NA', 'completionRate': 'NA', 'collectCount': 'NA',
                     'commentCount': 'NA', 'headurls': 'NA', 'cover': 'NA',
                     'isPromotion': 'NA', 'likeCount': 'NA', 'mainMvUrl': 'NA',
                     'shareCount': 'NA', 'viewCount': 'NA',
                     'userId': 'NA', 'userName': 'NA', 'time': 'NA',
                     'updateTime': 'NA', 'get_time': datetime.datetime.now()}, index=[0])
                csv_path = os.path.join(folder_path, str(accountId) + "_PhotoList.csv")
                PhotoList.to_csv(csv_path, index=False)
                time.sleep(2)

                # 无话题列表
                TopicList = pd.DataFrame(
                    {'accountId': accountId, 'kuaishouId': kuaishouId, 'userid': userid, 'index': index,
                     'collectCount': 'NA',
                     'commentCount': 'NA', 'count': 'NA', 'lastUpdateTime': 'NA',
                     'likeCount': 'NA', 'shareCount': 'NA',
                     'topic': 'NA', 'viewCount': 'NA',
                     'get_time': datetime.datetime.now()}, index=[0])
                csv_path = os.path.join(folder_path, str(accountId) + "_TopicList.csv")
                TopicList.to_csv(csv_path, index=False)
                time.sleep(2)

                # 无评论列表
                CmtList = pd.DataFrame({'accountId': accountId, 'kuaishouId': kuaishouId, 'index': index, 'userid': userid,
                                        'content': 'NA',
                                        'headurl': 'NA', 'time': 'NA', 'userId': 'NA',
                                        'get_time': datetime.datetime.now()}, index=[0])
                csv_path = os.path.join(folder_path, str(accountId) + "_CmtList.csv")
                CmtList.to_csv(csv_path, index=False)

                data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index': index, 'userid': userid,
                        'photolist': 'NA', 'phototopic': 'NA', 'photocmt': 'NA', 'photoagg': photoCount,
                        'accountOverview': 'skip',
                        'get_time': datetime.datetime.now()}
                cur.insert_one(data)
                time.sleep(5)

            elif 0 < photoCount <= 100:

                return1 = get_photolist(accountId, photolisturl, startTime, endTime)
                #return1 = get_photolists(accountId, photolisturl, i, startTime, endTime)
                time.sleep(2)
                x = x + 1
                # return2 = get_phototopic(accountId, phototopicyrl, startTime, endTime) # 修改
                # time.sleep(2)
                x = x + 1
                # return3 = get_photocmt(accountId, cmturl, startTime, endTime) # 修改
                # time.sleep(5)
                x = x + 1

                data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index': index, 'userid': userid,
                        'photolist': return1, 'photoagg': photoCount,
                        'accountOverview': 'skip',
                        'get_time': datetime.datetime.now()}
                cur.insert_one(data)
                time.sleep(12)

                #print(return1, return2, return3, return4, return5)

            elif photoCount > 100:

                page = math.ceil(photoCount / 100)
                for i in range(1, page + 1):
                    return1 = get_photolists(accountId, photolisturl, i, startTime, endTime)
                    time.sleep(2)
                    x = x + 1

                # return2 = get_phototopic(accountId, phototopicyrl, startTime, endTime) # 修改
                # time.sleep(2)
                x = x + 1
                # return3 = get_photocmt(accountId, cmturl, startTime, endTime) # 修改
                # time.sleep(10)
                x = x + 1

                data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index': index, 'userid': userid,
                        'photolist': page, 'photoagg': photoCount,
                        'accountOverview': 'skip',
                        'get_time': datetime.datetime.now()}
                cur.insert_one(data)


            print(index, accountId, userid, x)





