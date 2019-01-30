import os
from pathlib import Path
from io import BytesIO
import struct

from nonebot import on_command, CommandSession, permission, Scheduler
#from nonebot.scheduler import scheduled_job
from nonebot.log import logger
import requests
import datetime
import numpy as np

from . import DataBlock_pb2
from .plotplus import Plot

PERMITUSERS = {274555447, 409762147, 958495773, 1113651421, 2822905121, 1031986505,
               1067864739, 1306795502, 1178704631, 2801203606, 228573596,
               236693398, 314494604, 2643669184, 1137190844, 1048082999, 2054002374,
               1163601798}

def convert_time(fx:int):
    if fx in range(0, 10):
        fs = '00{}'.format(fx)
    elif fx in range(10, 100):
        fs = '0{}'.format(fx)
    else:
        fs = str(fx)
    return fs

def get_id(session:CommandSession):
    ctx = session.ctx
    return ctx['user_id']

def get_latest_run():
    now = datetime.datetime.utcnow()
    if now.hour >= 6 and now.hour <= 18:
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

def clip_data(lon, lat, data, lon_min, lon_max, lat_min, lat_max):
    left = np.where(lon >= lon_min)[0][0]
    right = np.where(lon <= lon_max)[0][-1]
    down = np.where(lat >= lat_min)[0][-1]
    up = np.where(lat <= lat_max)[0][0]
    return data[up - 1:down + 1, left - 1:right + 1]

pic_root = Path(r'D:\酷Q Pro\data\image')

@on_command('EC相态24')
async def get_xt24(session:CommandSession):
    url = 'http://10.1.64.146/picture/winter/{}/ecmwf/xt24/{}_{}.png'
    t = get_latest_run()
    output_path = ['NMCP', 'EC', 'XT24', t.strftime('%Y%m%d%H')]
    fin_path = recursive_create_dir(pic_root, output_path)
    for fxh in ['024', '048', '072', '096', '120', '144', '168', '192', '216', '240']:
        t_path = fin_path.joinpath('{}_{}.png'.format(t.strftime('%Y%m%d%H'), fxh))
        if not os.path.exists(t_path.as_posix()):
            await download(url.format(t.strftime('%Y%m%d%H'), t.strftime('%Y%m%d%H'), fxh), t_path.as_posix())
        await session.send('[CQ:image,file={}]'.format('/'.join(output_path) + '/' + t_path.name))

async def download(url, path, retry=3):
    for _ in range(retry):
        try:
            logger.info('Download start {}'.format(url))
            logger.info('Download path {}'.format(path))
            req = requests.get(url)
            content = req.content
            buf = open(path, 'wb')
            buf.write(content)
            buf.close()
            logger.info('Download complete {}'.format(url))
            return True
        except Exception as e:
            continue
    return

class MDFS_Grid:
    def __init__(self, filepath):
        if hasattr(filepath, 'read'):
            f = filepath
        else:
            f = open(filepath, 'rb')
        if f.read(4).decode() != 'mdfs':
            raise ValueError('Not valid mdfs data')
        self.datatype = struct.unpack('h', f.read(2))[0]
        self.model_name = f.read(20).decode('gbk').replace('\x00', '')
        self.element = f.read(50).decode('gbk').replace('\x00', '')
        self.data_dsc = f.read(30).decode('gbk').replace('\x00', '')
        self.level = struct.unpack('f', f.read(4))
        year, month, day, hour, tz = struct.unpack('5i', f.read(20))
        self.utc_time = datetime.datetime(year, month, day, hour) - datetime.timedelta(hours=tz)
        self.period = struct.unpack('i', f.read(4))
        start_lon, end_lon, lon_spacing, lon_number = struct.unpack('3fi', f.read(16))
        start_lat, end_lat, lat_spacing, lat_number = struct.unpack('3fi', f.read(16))
        lon_array = np.arange(start_lon, end_lon + lon_spacing, lon_spacing)
        lat_array = np.arange(start_lat, end_lat + lat_spacing, lat_spacing)
        isoline_start_value, isoline_end_value, isoline_space = struct.unpack('3f', f.read(12))
        f.seek(100, 1)
        block_num = lat_number * lon_number
        data = {}
        data['Lon'] = lon_array
        data['Lat'] = lat_array
        if self.datatype == 4:
            # Grid form
            grid = struct.unpack('{}f'.format(block_num), f.read(block_num * 4))
            grid_array = np.array(grid).reshape(lat_number, lon_number)
            data['Grid'] = grid_array
        elif self.datatype == 11:
            # Vector form
            norm = struct.unpack('{}f'.format(block_num), f.read(block_num * 4))
            angle = struct.unpack('{}f'.format(block_num), f.read(block_num * 4))
            norm_array = np.array(norm).reshape(lat_number, lon_number)
            angle_array = np.array(angle).reshape(lat_number, lon_number)
            # Convert stupid self-defined angle into correct direction angle
            corr_angle_array = 270 - angle_array
            corr_angle_array[corr_angle_array < 0] += 360
            data['Norm'] = norm_array
            data['Direction'] = corr_angle_array
        self.data = data

async def get_mdfs(directory, filename):
    _bytearray = DataBlock_pb2.ByteArrayResult()
    url = 'http://10.116.32.66:8080/DataService?requestType=getData&directory={}&fileName={}'.format(directory, filename)
    logger.info(url)
    req = requests.get(url)
    _bytearray.ParseFromString(req.content)
    return BytesIO(_bytearray.byteArray)

async def ec_t850_h500(fxhour:str):
    init_time = get_latest_run() + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    f_string = '{}_{}_EC_T850_H500.png'.format(get_latest_run().strftime('%Y%m%d%H'), fxhour)
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t850_data = await get_mdfs('ECMWF_HR/TMP/850/', fname)
    h500_data = await get_mdfs('ECMWF_HR/HGT/500/', fname)
    try:
        t850_mdfs = MDFS_Grid(t850_data)
        h500_mdfs = MDFS_Grid(h500_data)
    except Exception:
        return None
    t850 = t850_mdfs.data['Grid'][::-1]
    h500 = h500_mdfs.data['Grid'][::-1] * 10
    x, y = t850_mdfs.data['Lon'], t850_mdfs.data['Lat']
    georange = (0, 80, 50, 170)
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(300)
    p.setmap(projection='lcc', georange=(10, 50, 75, 150), lat_1=30, lat_2=35, lat_0=35, lon_0=105)
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.25)
    c = p.contourf(t850, gpfcmap='temp2')
    p.contour(h500, levels=np.arange(4000, 6000, 40), clabeldict={'levels':np.arange(4000, 6000, 40)}, color='black',
              lw=0.3)
    p.drawcoastline()
    p.drawprovinces()
    p.colorbar(c)
    p.title('ECMWF 850hPa Temperature, 500hPa Geopotential Height (Generated by QQbot)', nasdaq=False)
    p.timestamp(get_latest_run().strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_t2m(fxhour:str):
    init_time = get_latest_run() + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    f_string = '{}_{}_EC_T2M.png'.format(get_latest_run().strftime('%Y%m%d%H'), fxhour)
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_HR/TMP_2M/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(300)
    p.setmap(projection='lcc', georange=(10, 50, 75, 150), lat_1=30, lat_2=35, lat_0=35, lon_0=105)
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
    c = p.contourf(t2m_mdfs.data['Grid'][::-1], gpfcmap='tt.850t')
    p.drawcoastline()
    p.drawprovinces()
    p.colorbar(c)
    p.title('ECMWF 2m Temperature (Generated by QQbot)', nasdaq=False)
    p.timestamp(get_latest_run().strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_uv850_h500(fxhour:str):
    init_time = get_latest_run() + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    f_string = '{}_{}_EC_UV850_H500.png'.format(get_latest_run().strftime('%Y%m%d%H'), fxhour)
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    h500_data = await get_mdfs('ECMWF_HR/HGT/500/', fname)
    u850_data = await get_mdfs('ECMWF_HR/UGRD/850/', fname)
    v850_data = await get_mdfs('ECMWF_HR/VGRD/850/', fname)
    try:
        h500_mdfs = MDFS_Grid(h500_data)
        u850_mdfs = MDFS_Grid(u850_data)
        v850_mdfs = MDFS_Grid(v850_data)
    except Exception as e:
        logger.warn(e)
        return None
    x, y = h500_mdfs.data['Lon'], h500_mdfs.data['Lat']
    h500 = clip_data(x, y, h500_mdfs.data['Grid'], 90, 125, 17, 42)[::-1] * 10
    u850 = clip_data(x, y, u850_mdfs.data['Grid'], 90, 125, 17, 42)[::-1] * 1.94
    v850 = clip_data(x, y, v850_mdfs.data['Grid'], 90, 125, 17, 42)[::-1] * 1.94
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(300)
    p.setmap(georange=(17, 42, 90, 125))
    p.setxy((17, 42, 90, 125), 0.25)
    c = p.contourf(h500, gpfcmap='geopo')
    lvl = [4600, 4660, 4720, 4780, 4840, 4900, 4960, 5020, 5060, 5100, 5140, 5180, 5220, 5260, 5300, 5340,
           5380, 5420, 5460, 5500, 5540, 5580, 5620, 5660, 5700, 5740, 5780, 5820, 5860, 5880, 5900, 5920,
           5940, 5970, 6000]
    p.contour(h500, levels=lvl, clabeldict={'levels':lvl}, color='black', lw=0.4, alpha=0.6)
    p.barbs(u850, v850, num=20)
    p.drawcoastline()
    p.drawprovinces()
    p.colorbar(c)
    p.title('ECMWF 850hPa Wind Barbs, 500hPa Geopotential Height (Generated by QQbot)', nasdaq=False)
    p.timestamp(get_latest_run().strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_asnow(fxhour:str):
    init_time = get_latest_run() + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    f_string = '{}_{}_EC_ASNOW.png'.format(get_latest_run().strftime('%Y%m%d%H'), fxhour)
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_HR/ASNOW/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(300)
    p.setmap(georange=(17, 42, 90, 125))
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
    c = p.contourf(t2m_mdfs.data['Grid'][::-1], gpfcmap='wxb.snow')
    p.drawcoastline()
    p.drawprovinces()
    p.colorbar(c)
    p.title('ECMWF Accumulated Total Precipitation (Snow) (mm) (Generated by QQbot)', nasdaq=False)
    p.timestamp(get_latest_run().strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

FUNC_CONV = {'T2M':ec_t2m, 'T850H500':ec_t850_h500, 'UV850H500':ec_uv850_h500, 'ASNOW':ec_asnow}

@on_command('EC')
async def call_ec_func(session:CommandSession):
    ids = get_id(session)
    if ids not in PERMITUSERS:
        await session.send('Permission denied')
        raise PermissionError('Permission denied')
    raw = session.ctx['raw_message'].split('EC')[1].strip()
    command = raw.split(' ')
    func = FUNC_CONV[command[0]]
    try:
        fx = int(command[1])
    except Exception as e:
        await session.send(e)
    fs = convert_time(fx)
    fpath = await func(fs)
    if fpath:
        await session.send('[CQ:image,file={}]'.format(fpath))
    else:
        await session.send('当前预报时效无数据')
