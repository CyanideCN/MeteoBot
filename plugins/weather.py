import re
import base64
import zlib
import json
from io import BytesIO, StringIO
import ast
import pickle

from nonebot import on_command, CommandSession, logger
import requests
from bs4 import BeautifulSoup
import numpy as np
import metpy.calc as mpcalc
from metpy.units import units

from .database import DBRecord
from .autopic import get_id

with open('station_reverse.pickle', 'rb') as buf:
    index = pickle.load(buf)

@on_command('查天气', only_to_me=False)
async def weather(session:CommandSession):
    city = session.get('city', prompt='你想查询哪个站的实况呢？')
    if not city.isnumeric():
        try:
            raw = index[city]
        except KeyError:
            await session.send('未知站号/站名:{}'.format(city))
    else:
        raw = city
    ids = get_id(session)
    db = DBRecord()
    ctx = session.ctx
    if ids == 1178704631:
        if ctx['message_type'] == 'private':
            pass
        else:
            group_id = ctx['group_id']
            db.blacklist(ids, group_id, raw)
        await session.send('Unknown error occurred')
        raise PermissionError('Unknown error')
    else:
        if ctx['message_type'] != 'private':
            group_id = ctx['group_id']
            comm = db.get_bl_command(1178704631, group_id)
            if comm:
                if raw in comm['command']:
                    await session.send('Unknown error occurred')
                    raise PermissionError('Unknown error')
    weather_report = await get_weather_of_city(raw)
    db = DBRecord()
    db.weather(ids, raw)
    await session.send(weather_report)

async def get_weather_of_city(code):
    logger.info(code)
    try:
        code = ast.literal_eval(code)
        req = requests.get('http://q-weather.info/weather/{}/realtime/'.format(code))
    except Exception as e:
        import traceback
        return traceback.format_exc()
    soup = BeautifulSoup(req.content)
    tds = soup.find_all('td')
    if len(tds) == 0:
        return '无数据'
    td_strip = np.array([i.contents[0] for i in tds])
    fin = np.dstack([td_strip[0::3],td_strip[1::3],td_strip[2::3]])[0]
    seg = [' '.join(i) for i in fin]
    return soup.find_all('h1')[0].contents[0] + '\n' + '\n'.join(seg)

@weather.args_parser
async def _(session:CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.current_key:
        session.args[session.current_key] = stripped_arg
    elif stripped_arg:
        session.args['city'] = stripped_arg

@on_command('露点', only_to_me=False)
async def dewp(session:CommandSession):
    raw = session.ctx['raw_message'].split('露点')[1].strip()
    parts = raw.split(' ')
    temp = ast.literal_eval(parts[0]) * units.degC
    rh = ast.literal_eval(parts[1]) * units.percent
    td = mpcalc.dewpoint_rh(temp, rh)
    await session.send(str(np.round_(td.magnitude, 2)))