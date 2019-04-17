import re
import base64
import zlib
import json
from io import BytesIO, StringIO
import ast

from nonebot import on_command, CommandSession, logger
import requests
from bs4 import BeautifulSoup
import numpy as np
import metpy.calc as mpcalc
from metpy.units import units

from .database import DBRecord
from .autopic import get_id

@on_command('查天气', only_to_me=False)
async def weather(session:CommandSession):
    city = session.get('city', prompt='你想查询哪个站的实况呢？')
    weather_report = await get_weather_of_city(city)
    ids = get_id(session)
    db = DBRecord()
    db.weather(ids, city)
    await session.send(weather_report)

async def get_weather_of_city(code):
    try:
        code = ast.literal_eval(code)
        req = requests.get('http://q-weather.info/weather/{}/realtime/'.format(code))
    except Exception as e:
        return str(e)
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