# 获取新快用户-品类商品-商品列表 近7天
import csv
import json
import os
from pprint import pprint
import pandas as pd
import requests
import datetime

from pymongo import MongoClient
# # 链接数据库
con = MongoClient('127.0.0.1', 27017)
db = con.kuaishou_combine
cur = db.merchant0723  # 修改

# 商品列表
merchantUrl = 'https://gw.newrank.cn/api/xk/xdnphb/nr/cloud/ks/material/photoDetailSearch'

# 请求头设置
payloadHeader = {
    'Origin': 'https://xk.newrank.cn',
    'Connection': 'keep-alive',
    'Referer': 'https://xk.newrank.cn/',
    'Host': 'gw.newrank.cn',
    'N-Token': '96a7725673b545599bb1ec18898ad603',
    'Content-Type': 'application/json;charset=UTF-8',
    'Cookie': 'Hm_lvt_a19fd7224d30e3c8a6558dcb38c4beed=1695219658; acw_tc=781bad2016952837935355968e4e261cb0171d81eb1157e7962c8daa09aeb9; token=673E9091131D40EAB4BBB1B576D6AEAA; NR_MAIN_SOURCE_RECORD={"locationSearch":"","locationHref":"https://xk.newrank.cn/conversion","referrer":"https://xk.newrank.cn/buy/pay/order/2/12"}; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22nr_b9bag954g%22%2C%22first_id%22%3A%2218ab2d76f84e9e-032cab168b7bae4-19525634-1484784-18ab2d76f85c9b%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%2218ab2d76f84e9e-032cab168b7bae4-19525634-1484784-18ab2d76f85c9b%22%7D; Hm_lpvt_a19fd7224d30e3c8a6558dcb38c4beed=1695284865; amp_6e403e=hi5IWT4x_GI6BLvJYmFlbg...1hardks3i.1hardks3i.0.0.0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
}

# 修改
df = pd.read_csv('/Users/lyh/Desktop/AAAAARMBS/Kuaishou/data/xinkuai/down_video/promote_video0723.csv',
                header=0, skiprows = 0)  # 修改  15189
columns_name = ['accountId', 'kuaishouId', 'userid', 'vid_name', 'time', 'indexid']
df.columns = columns_name
merged_df = pd.DataFrame()

#创建保存数据的csv
# 定义列名和数据

# 定义文件路径
file_path = "/Users/lyh/Desktop/AAAAARMBS/Kuaishou/data/xinkuai/combined_data/0723/Merchant0723.csv"  # 修改 修改

for index, row in df.iterrows():
    indexid = row['indexid']
    accountId = str(row['accountId'])
    kuaishouId = row['kuaishouId']
    userid = row['userid']
    vid_name = row['vid_name']  #url="https://www.douyin.com/video/7180961410762001724"  https://video.kuaishou.com/short-video/5236842043040459608
    vid_id = vid_name[vid_name.index("V") + 1:]

    # payloadData数据
    payloadData = {"photoId": vid_id}

    # 页面响应
    import time
    time.sleep(13)

    r = requests.post(merchantUrl, data=json.dumps(payloadData), headers=payloadHeader)
    dumpJsonData = json.dumps(payloadData)
    print(f"dumpJsonData = {dumpJsonData}")
    res = requests.post(merchantUrl, data=dumpJsonData, headers=payloadHeader, allow_redirects=True) # 下面这种直接填充json参数的方式也OK # # 下载超时 timeOut = 25 # res = requests.post(postUrl, json=payloadData, headers=header)
    print(f"responseTime = {datetime.datetime.now()}, statusCode = {res.status_code}, res text = {res.text}")
    json_data = json.loads(res.text) # 限制每天访问363条？ 超出使用条数
    pprint(json_data)

    try:
        anaDel = json_data['data']['anaDel']
    except Exception as e:
        anaDel = 'wrong'
        print(e)

    try:
        anaTags = json_data['data']['anaTags']
    except Exception as e:
        anaTags = 'wrong'
        print(e)


    try:
        caption = json_data['data']['caption']
    except Exception as e:
        caption = 'wrong'
        print(e)

    try:
        collectCount = json_data['data']['collectCount']
    except Exception as e:
        collectCount = 'wrong'
        print(e)

    try:
        commentCount = json_data['data']['commentCount']
    except Exception as e:
        commentCount = 'wrong'
        print(e)

    try:
        cover = json_data['data']['cover']
    except Exception as e:
        cover = 'wrong'
        print(e)

    try:
        coverType = json_data['data']['coverType']
    except Exception as e:
        coverType = 'wrong'
        print(e)

    try:
        duration = json_data['data']['duration']
    except Exception as e:
        duration = 'wrong'
        print(e)

    try:
        forwardCount = json_data['data']['forwardCount']
    except Exception as e:
        forwardCount = 'wrong'
        print(e)

    try:
        headurls = json_data['data']['headurls']
    except Exception as e:
        headurls = 'wrong'
        print(e)

    try:
        insertTime = json_data['data']['insertTime']
    except Exception as e:
        insertTime = 'wrong'
        print(e)

    try:
        isPromotion = json_data['data']['isPromotion']
    except Exception as e:
        isPromotion = 'wrong'
        print(e)

    try:
        kwaiId = json_data['data']['kwaiId']
    except Exception as e:
        kwaiId = 'wrong'
        print(e)

    try:
        likeCount = json_data['data']['likeCount']
    except Exception as e:
        likeCount = 'wrong'
        print(e)

    try:
        mtype = json_data['data']['mtype']
    except Exception as e:
        mtype = 'wrong'
        print(e)

    try:
        music = json_data['data']['music']
    except Exception as e:
        music = 'wrong'
        print(e)

    try:
        photoId = json_data['data']['photoId']
    except Exception as e:
        photoId = 'wrong'
        print(e)

    try:
        poi = json_data['data']['poi']
    except Exception as e:
        poi = 'wrong'
        print(e)

    try:
        screenType = json_data['data']['screenType']
    except Exception as e:
        screenType = 'wrong'
        print(e)

    try:
        shareCount = json_data['data']['shareCount']
    except Exception as e:
        shareCount = 'wrong'
        print(e)

    try:
        shareInfo = json_data['data']['shareInfo']
    except Exception as e:
        shareInfo = 'wrong'
        print(e)

    try:
        status = json_data['data']['status']
    except Exception as e:
        status = 'wrong'
        print(e)

    try:
        subMtype = json_data['data']['subMtype']
    except Exception as e:
        subMtype = 'wrong'
        print(e)

    try:
        time = json_data['data']['time']
    except Exception as e:
        time = 'wrong'
        print(e)

    try:
        viewCount = json_data['data']['viewCount']
    except Exception as e:
        viewCount = 'wrong'
        print(e)

    try:
        updateTime = json_data['data']['updateTime']
    except Exception as e:
        updateTime = 'wrong'
        print(e)

    try:
        adType = json_data['data']['merchant']['adType']
    except Exception as e:
        adType = 'wrong'
        print(e)

    try:
        description = json_data['data']['merchant']['description']
    except Exception as e:
        description = 'wrong'
        print(e)

    try:
        flagJson = json_data['data']['merchant']['flagJson']
    except Exception as e:
        flagJson = 'wrong'
        print(e)

    try:
        itemId = json_data['data']['merchant']['itemId']
    except Exception as e:
        itemId = 'wrong'
        print(e)

    try:
        itemSourceType = json_data['data']['merchant']['itemSourceType']
    except Exception as e:
        itemSourceType = 'wrong'
        print(e)

    try:
        jumpUrl = json_data['data']['merchant']['jumpUrl']
    except Exception as e:
        jumpUrl = 'wrong'
        print(e)

    try:
        price = json_data['data']['merchant']['price']
    except Exception as e:
        price = 'wrong'
        print(e)

    try:
        sales = json_data['data']['merchant']['sales']
    except Exception as e:
        sales = 'wrong'
        print(e)

    try:
        sourceType = json_data['data']['merchant']['sourceType']
    except Exception as e:
        sourceType = 'wrong'
        print(e)

    try:
        thumb = json_data['data']['merchant']['thumb']
    except Exception as e:
        thumb = 'wrong'
        print(e)

    try:
        title = json_data['data']['merchant']['title']
    except Exception as e:
        title = 'wrong'
        print(e)

    try:
        url = json_data['data']['merchant']['url']
    except Exception as e:
        url = 'wrong'
        print(e)


    data = {'accountId': accountId, 'kuaishouId': kuaishouId, 'index': index, 'userid': userid, 'vid_name': vid_name, 'photoId': photoId,
                                 'anaDel': anaDel, 'anaTags': anaTags, 'caption': caption,
                                 'collectCount': collectCount, 'commentCount': commentCount,
                                 'likeCount': likeCount, 'shareCount': shareCount, 'viewCount': viewCount,
                                 'cover': cover, 'coverType': coverType, 'duration': duration, 'forwardCount': forwardCount,
                                 'headurls': headurls, 'insertTime': insertTime, 'isPromotion': isPromotion,
                                 'kwaiId': kwaiId,  'mtype': mtype, 'music': music, 'poi': poi, 'screenType': screenType,
                                 'shareInfo': shareInfo, 'status': status, 'subMtype': subMtype, 'time': time, 'updateTime': updateTime,
                                 'adType': adType, 'description': description, 'flagJson': flagJson, 'itemId': itemId,
                                 'itemSourceType': itemSourceType,
                                 'jumpUrl': jumpUrl, 'price': price, 'sales': sales, 'sourceType': sourceType,
                                 'thumb': thumb, 'title': title, 'url': url,
                                 'get_time': datetime.datetime.now()}
    cur.insert_one(data)


    print(indexid, vid_name, index)



