# coding=utf-8
__author__ = 'kai_k_000'

# nohup python -u main.py sina > ./log.txt 2>&1 &
# nohup python -u main.py ws > ./log.txt 2>&1 &
# nohup python -u main.py zj > ./log.txt 2>&1 &

# python  main.py sina test 1001
# nohup python -u main.py sina older 1001 > ./log.txt 2>&1 &

_SINA_SAMPLE__ = "http://zhibo.sina.com.cn/api/zhibo/feed?callback=jQuery111206309269858793495_1552133471689&" \
            "page=1&page_size=20&zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=20&_=1552133471709"

_WS_SAMPLE__ = "https://api.wallstreetcn.com/apiv1/content/lives?channel=global-channel&client=pc" \
                 "&cursor=1552096956&limit=20&first_page=false&accept=live,vip-live"


__SINA_MAINPAGE__ = "http://finance.sina.com.cn/7x24/"
__WS_MAINPAGE__ = "https://wallstreetcn.com/live/global"
__ZJ_MAINPAGE__ = "http://live.stock.cnfol.com/"


__RECORDFILE_SINA__ = "./file/tmp_sina.csv"
__RECORDFILE_WS__ = "./file/tmp_ws.csv"
__RECORDFILE_ZJ__ = "./file/tmp_zj.csv"

__SINA_TAG__ = "sina"
__SINA_TAG_ID__ = 0
__WS_TAG__ = "ws"

__ZJ_TAG__ = "cnfol"



__SINA_LIVE_ADD__ = "http://zhibo.sina.com.cn/api/zhibo/feed?callback=%s_%s&" \
              "page=%d&page_size=%d&zhibo_id=152&tag_id=%d&dire=f&dpc=1&pagesize=%d&_=%s"
__SINA_CALLBACK__ = "jQuery111206309269858793495"

__WS_LIVE_ADD__ = "https://api.wallstreetcn.com/apiv1/content/lives?channel=global-channel&" \
                  "client=pc&cursor=%d&limit=20&first_page=false&accept=live,vip-live"

__ZJ_LIVE_ADD__ = "http://live.stock.cnfol.com/stocklive/livedata"

##contentHASH,'id', 'source', 不要更改位置，record.py需要
__FIELDNAMES__ = ['contentHASH', 'id', 'source', 'content', 'created_at', 'tag_id', 'tag_name', 'tag_id2', 'tag_name2']

__ERROR_RETRY__ = 2
##华尔街的api cursor 其实是时间

##中金从3月1日起有数据

import record
import network
import time
import datetime
import random
import os
import sys

class someError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def getTimeSeed(microsecond=True):
    if microsecond:
        return str(time.time()*1000)[:13]
    else:
        return str(time.time()*1000)[:10]


def unixtTime2StringTime(strTime):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(strTime)))

def getPreviousDate(dateStr):
    return str(datetime.datetime.strptime(dateStr,'%Y-%m-%d').date() - datetime.timedelta(days=1))


def sinaMessageRequest(net, rf, page):
# __SINA_LIVE_ADD__ = "http://zhibo.sina.com.cn/api/zhibo/feed?callback=%s_%s&" \
#               "page=%d&page_size=%d&zhibo_id=152&tag_id=%d&dire=f&dpc=1&pagesize=%d&_=%s"
    seed = getTimeSeed(microsecond=False)
    reqAddr = __SINA_LIVE_ADD__ % (__SINA_CALLBACK__,
                                   getTimeSeed(microsecond=False), page, 20, __SINA_TAG_ID__, 20, getTimeSeed(microsecond=False))
    for i in range(__ERROR_RETRY__):
        print(i, reqAddr)
        resStr = net.getResponseData(reqAddr)
        resDic = rf.getJsonObject(resStr, __SINA_CALLBACK__ + "_" + seed)
        resCode = resDic["result"]["status"]["code"]
        if resCode == 0:
            break
        if i == __ERROR_RETRY__:
            raise someError("__ERROR_RETRY__:",resCode,resDic["result"]["status"]["msg"] )

    # page_info = result["result"]["data"]["feed"]["page_info"]
    rows = []
    for msg in resDic["result"]["data"]["feed"]["list"]:
        row = {'contentHASH': rf.getMD5(msg['rich_text']), 'id': msg['id'],
               'source': __SINA_TAG__, 'content': msg['rich_text'],
               'created_at': msg['create_time']}

        if len(msg['tag']) >=1:
            row['tag_id'] = msg['tag'][0]['id']
            row['tag_name'] = msg['tag'][0]['name']
        if len(msg['tag']) >1:
            row['tag_id2'] = msg['tag'][1]['id']
            row['tag_name2'] = msg['tag'][1]['name']
        rows.append(row)

    return rf.addARows(rows)


def sinaCollectorProcess(startPage=1):
    sinaNet = network.net()

    if sinaNet.loadCookie(__SINA_MAINPAGE__) is None:
        raise network.networkError("__SINA_MAINPAGE__")

    rf = record.recordfile(__RECORDFILE_SINA__, __FIELDNAMES__)

    page = startPage
    insertCusumError = 0
    lockfile = open('./lockPage_sina','w+')
    while page:
        lockfile.seek(0)
        lockfile.write(str(page))
        lockfile.flush()
        interval = random.uniform(2, 90)
        print(unixtTime2StringTime(time.time()), page, interval)
        result = sinaMessageRequest(sinaNet, rf, page)
        print(result)
        for status in result:
            insertCusumError +=1
            if status == record.insertStatus.Success:
                insertCusumError=0

        if insertCusumError >= 39:  # 往前追溯时，碰到连续两页无新内容时，说明更新完毕
            raise someError("insertCusumError")

        if len(sys.argv) >= 3:  # 测试选项时，仅试试当前页面是否已全部获取。不保存
            if sys.argv[2] == 'test':
                if insertCusumError < 20:
                    raise someError("test error: exist NEW  Cusum" + str(insertCusumError) + "page:" + str(page))
                else:
                    raise someError("test past: ALL existed  Cusum" + str(insertCusumError) + "page:" + str(page))

        page += 1

        time.sleep(interval)


def wallstreetMessageRequest(net, rf, startTime):
    net = network.net()
    if net.loadCookie(__WS_MAINPAGE__) is None:
        raise network.networkError("__WS_MAINPAGE__")


    reqAddr = __WS_LIVE_ADD__ % (startTime)

    for i in range(__ERROR_RETRY__):
        print(i, reqAddr)
        resStr = net.getResponseData(reqAddr)
        resDic = rf.getJsonObject(resStr, None)
        resCode = resDic["code"]
        if resCode == 20000: # 请求成功跳出重试循环
            break
        if i == __ERROR_RETRY__:
            raise someError("__ERROR_RETRY__:", resCode, resDic["message"])

    items = resDic['data']['items']

    if items is None:
        print(resStr)
        raise someError('NULL Content,maybe wrong cusor')

    rows = []
    for item in items:
        if item['is_calendar']:  # is_calendar时，本条记录不要
            continue
        content = item['content_text'].replace('\n','')
        row = {'contentHASH': rf.getMD5(content), 'id': item['id'],
               'source': __WS_TAG__, 'content': content,
               'created_at': unixtTime2StringTime(item['display_time'])}

        row['tag_name'] = item['score']

        channels = item['channels']
        if 'global-channel' in channels:
            channels.remove('global-channel')
        row['tag_name2'] = '|'.join(item['channels'])

        rows.append(row)
    nextCursor= resDic['data']['next_cursor']

    return rf.addARows(rows), int(nextCursor)

def wallstreetCollectorProcess(startTime=1552490575):
    wsNet = network.net()

    if wsNet.loadCookie(__WS_MAINPAGE__) is None:
        raise network.networkError("__WS_MAINPAGE__")

    rf = record.recordfile(__RECORDFILE_WS__, __FIELDNAMES__)

    cursor = startTime
    insertCusumError = 0
    lockfile = open('./lockPage_ws','w+')
    while cursor:
        lockfile.seek(0)
        lockfile.write(str(cursor))
        lockfile.flush()
        interval = random.uniform(2, 90)
        print(unixtTime2StringTime(time.time()), cursor, interval)
        result,nextCursor = wallstreetMessageRequest(wsNet, rf, cursor)
        print(result)
        for status in result:
            insertCusumError +=1
            if status == record.insertStatus.Success:
                insertCusumError=0
        if insertCusumError >= 25:
            raise someError("insertCusumError")
        cursor = nextCursor

        time.sleep(interval)


def cnfolMessageRequest(net, rf, dateCursor='2018-03-01'):
    net = network.net()
    if net.loadCookie(__ZJ_MAINPAGE__) is None:
        raise network.networkError("__ZJ_MAINPAGE__")


    for i in range(__ERROR_RETRY__):
        print(i, __ZJ_LIVE_ADD__)
        resStr = net.getResponseData(__ZJ_LIVE_ADD__, {'livedate':dateCursor, 'order':1})
        resList = rf.getHTMLObject(resStr)
        if len(resList) > 0: # 请求成功跳出重试循环
            break

        if i == __ERROR_RETRY__:
            raise someError("__ERROR_RETRY__:")

    rows = []
    for item in resList:
        content = item[1]
        createdTime = dateCursor + ' ' + item[0] + ':00'
        row = {'contentHASH': rf.getMD5(content), 'id': '',
               'source': __ZJ_TAG__, 'content': content,
               'created_at': createdTime}
        rows.append(row)

    return rf.addARows(rows)

def cnfolCollectorProcess(startDateCursor='2018-03-01'):
    zjNet = network.net()

    if zjNet.loadCookie(__ZJ_MAINPAGE__) is None:
        raise network.networkError("__ZJ_MAINPAGE__")

    rf = record.recordfile(__RECORDFILE_ZJ__, __FIELDNAMES__)

    cursor = startDateCursor
    insertCusumError = 0
    lockfile = open('./lockPage_zj','w+')
    while cursor:
        lockfile.seek(0)
        lockfile.write(str(cursor))
        lockfile.flush()
        interval = random.uniform(2, 90)
        print(unixtTime2StringTime(time.time()), cursor, interval)
        result = cnfolMessageRequest(zjNet, rf, cursor)
        print(result)
        for status in result:
            insertCusumError +=1
            if status == record.insertStatus.Success:
                insertCusumError=0
        if insertCusumError >= 25:
            raise someError("insertCusumError")
        if cursor == '2018-03-01':
            raise someError("last date have data,had get all msg")
        cursor = getPreviousDate(cursor)

        time.sleep(interval)

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        raise someError("need 1 param, must be 'sina' or 'ws'")

    if sys.argv[1] == 'sina':
        print('start sinaCollectorProcess')
        if len(sys.argv) >= 3:
            if sys.argv[2] == 'test':  # 测试选项，第4选项为起始页
                sinaCollectorProcess(int(sys.argv[3]))
            elif sys.argv[2] == 'older':  # 在旧的基础上往前追溯
                if os.path.isfile('./lockPage_sina'):
                    raise someError("check lockPage_sina file, it cant be exist")
                sinaCollectorProcess(int(sys.argv[3]))
        else:
            if os.path.isfile('./lockPage_sina'):
                raise someError("check lockPage_sina file, it cant be exist")
            sinaCollectorProcess(1)

    if sys.argv[1] == 'ws':
        print('ws')
        if os.path.isfile('./lockPage_ws'):
            raise someError("check lockPage_ws file, it cant be exist")
        # 从15分钟前数据开始
        wallstreetCollectorProcess(int(getTimeSeed(microsecond=False))-900)

    if sys.argv[1] == 'zj':
        print('zj')
        if os.path.isfile('./lockPage_zj'):
            raise someError("check lockPage_zj file, it cant be exist")
        # 从昨天开始
        cnfolCollectorProcess(getPreviousDate(time.strftime('%Y-%m-%d', time.localtime(time.time()))))

        print('end')
