import os
import time
import datetime
from pathlib import Path
import pickle

from nonebot import on_command, CommandSession
import requests

with open('radar.pickle', 'rb') as buf:
    radar_dict = pickle.load(buf)

headers = {'Referer': 'http://www.observation-cma.com/html1/folder/1612/261-1.htm?product_code=TQLD_ACHN',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}

img_serach = 'http://www.observation-cma.com/ImgsSearchs.do?act=doSearchImgs&product_id=16120185&cate_type=QREF&start_date={}&end_date={}&t={}'
radar_pic = 'http://www.observation-cma.com/mocimg/tqld/single_station/{}/{}/{}/{}.{}000.{}.{}00.PNG'
#http://www.observation-cma.com/mocimg/tqld/single_station/Z9010/CREF/20190430/Z9010.CREF000.20190430.030000.PNG

def get_last_img_time():
    time_nows = time.time()
    time_now = time.localtime(time_nows)
    dtime_now = datetime.datetime(*time_now[:6])
    q_start = dtime_now - datetime.timedelta(days=1)
    fmt = '%Y-%m-%d %H:%M:%S'
    req = requests.get(img_serach.format(q_start.strftime(fmt), time.strftime(fmt, time_now), int(time_nows * 100)), headers=headers)
    js = req.json()
    last_img_time = js['img_list'][0]['img_time']
    return datetime.datetime.strptime(last_img_time, '%Y-%m-%d %H:%M')

def download_pic(radar_code, prod):
    dtime = get_last_img_time() - datetime.timedelta(hours=8)
    purl = radar_pic.format(radar_code, prod, dtime.strftime('%Y%m%d'), radar_code, prod, dtime.strftime('%Y%m%d'), dtime.strftime('%H%M'))
    fname = purl.split('/')[-1]
    req = requests.get(purl)
    root = r'D:\酷Q Pro\data\image\radar'
    prim = os.path.join(root, radar_code)
    sec = os.path.join(prim, dtime.strftime('%Y%m%d'))
    if not os.path.exists(prim):
        os.mkdir(prim)
    if not os.path.exists(sec):
        os.mkdir(sec)
    if b'<!DOCTYPE html>' in req.content:
        return None
    f = open(os.path.join(sec, fname), 'wb')
    f.write(req.content)
    return os.path.join('radar', radar_code, dtime.strftime('%Y%m%d'), fname)

@on_command('雷达', only_to_me=False)
async def rad(session:CommandSession):
    corr = {'R':'QREF', 'CR':'CREF', 'VIL':'VIL', 'OHP':'OHP'}
    ctx = session.ctx
    raw = ctx['raw_message'].split('雷达')[1].strip()
    comm_list = raw.split(' ')
    radar_code = radar_dict[comm_list[1]]
    try:
        fp = download_pic(radar_code, corr[comm_list[0]])
        if fp is None:
            await session.send('无数据')
        else:
            await session.send('[CQ:image,file={}]'.format(fp))
    except Exception:
        import traceback
        await session.send(traceback.format_exc())