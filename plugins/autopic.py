import os
import shutil
from pathlib import Path

from nonebot import on_command, CommandSession, permission
from nonebot.log import logger
import requests
import datetime

def get_latest_run():
    now = datetime.datetime.utcnow()
    if now.hour > 7:
        return datetime.datetime(now.year, now.month, now.day, 0)
    else:
        return datetime.datetime(now.year, now.month, now.day, 0) - datetime.timedelta(hours=12)

def recursive_create_dir(path, new_folder_list):
    for i in new_folder_list:
        path = path.joinpath(i)
        try:
            path.mkdir()
        except FileExistsError:
            pass
    return path

pic_root = Path(r'D:\酷Q Pro\data\image')

@on_command('EC相态24', permission=permission.SUPERUSER)
async def get_xt24(session:CommandSession):
    fxh = session.get('fxh')
    logger.info('Enter xt24')
    if not fxh:
        await session.send('不合法预报时效')
        raise ValueError('x')
    url = 'http://10.1.64.146/picture/winter/{}/ecmwf/xt24/{}_{}.png'
    t = get_latest_run()
    output_path = ['NMCP', 'EC', 'XT24', t.strftime('%Y%m%d%H')]
    fin_path = recursive_create_dir(pic_root, output_path)
    fin_path = fin_path.joinpath('{}_{}.png'.format(t.strftime('%Y%m%d%H'), fxh))
    if not os.path.exists(fin_path.as_posix()):
        await download(url.format(t.strftime('%Y%m%d%H'), t.strftime('%Y%m%d%H'), fxh), fin_path)
    await session.send('[CQ:image,file={}]'.format('/'.join(output_path) + '/' + fin_path.name))

@get_xt24.args_parser
def nwparser(session:CommandSession):
    fxh = int(session.current_arg_text.strip())
    if fxh % 24 != 0:
        session.args['fxh'] = None
    if fxh in range(0, 10):
        s = '00' + str(fxh)
    elif fxh in range(10, 100):
        s = '0' + str(fxh)
    else:
        s = fxh
    if session.current_key:
        session.args[session.current_key] = s
    else:
        session.args['fxh'] = s

async def download(url, path, retry=3):
    for _ in range(retry):
        try:
            req = requests.get(url)
            content = req.content
            buf = open(path, 'wb')
            buf.write(content)
            buf.close()
            return True
        except Exception as e:
            continue
    return