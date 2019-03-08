import os
from pathlib import Path
from io import BytesIO
import struct
from ctypes import *

from nonebot import on_command, CommandSession, permission, Scheduler
#from nonebot.scheduler import scheduled_job
from nonebot.log import logger
import requests
import datetime
import numpy as np
import matplotlib.colors as mcolor
import matplotlib.pyplot as plt
import metpy.calc as mpcalc
from metpy.units import units
#from scipy import ndimage

from . import DataBlock_pb2
from .plotplus import Plot

dll = CDLL(r'C:\Users\27455\source\repos\C++\SI_DLL\x64\Release\SI_DLL.dll')
dll.showalter_index.restype = c_double
dll.lifted_index.restype = c_double

def SI_wrapper(t850, td850, t500):
    return dll.showalter_index(c_double(t850), c_double(td850), c_double(t500))

def LI_wrapper(t850, td850, t500):
    return dll.lifted_index(c_double(t850), c_double(td850), c_double(t500))

PERMITUSERS = {274555447, 474463886, 228573596, 1287389600, 2054002374,
               '#1163601798', 1137190844, 314494604, 1306795502}

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

def parse_shell_command(command:str):
    left_v, right_v = command[2:].split('=')
    d = dict()
    d[left_v.lower()] = eval(right_v)
    return d

def get_latest_run(offset=6):
    now = datetime.datetime.utcnow()
    if now.hour >= offset and now.hour <= (24 - offset):
        return datetime.datetime(now.year, now.month, now.day, 0)
    elif now.hour < offset:
        return datetime.datetime(now.year, now.month, now.day, 0) - datetime.timedelta(hours=12)
    else:
        return datetime.datetime(now.year, now.month, now.day, 0) + datetime.timedelta(hours=12)

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
    return data[up:down+1, left:right+1]

def clip_data_sh(lon, lat, data, lon_min, lon_max, lat_min, lat_max, x1_offset=0, x2_offset=0, x3_offset=0, x4_offset=0):
    left = np.where(lon >= lon_min)[0][0]
    right = np.where(lon <= lon_max)[0][-1]
    down = np.where(lat >= lat_min)[0][0]
    up = np.where(lat <= lat_max)[0][-1]
    return data[down+x1_offset:up+2+x2_offset, left-1+x3_offset:right+1+x4_offset]

def quick_round(number):
    return np.round_(number, decimals=4)

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
        lon_array = np.arange(quick_round(start_lon), quick_round(end_lon) + quick_round(lon_spacing), quick_round(lon_spacing))
        lat_array = np.arange(quick_round(start_lat), quick_round(end_lat) + quick_round(lat_spacing), quick_round(lat_spacing))
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
        f.close()

async def get_mdfs(directory, filename):
    base = Path(r'D:\Meteorology\MDFS')
    base = base.joinpath(directory)
    date = filename.split('.')[0]
    base = base.joinpath(date)
    if not os.path.exists(base):
        os.makedirs(base)
    base = base.joinpath(filename)
    logger.info(base)
    if not os.path.exists(base):
        _bytearray = DataBlock_pb2.ByteArrayResult()
        url = 'http://10.116.32.66:8080/DataService?requestType=getData&directory={}&fileName={}'.format(directory, filename)
        logger.info(url)
        req = requests.get(url)
        _bytearray.ParseFromString(req.content)
        try:
            MDFS_Grid(BytesIO(_bytearray.byteArray))
            f = open(base.as_posix(), 'wb')
            f.write(_bytearray.byteArray)
            f.close()
            return BytesIO(_bytearray.byteArray)
        except:
            return None
    else:
        f = open(base.as_posix(), 'rb')
        return f

async def ec_t850_h500(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    f_string = '{}_{}_EC_T850_H500.png'.format(lr.strftime('%Y%m%d%H'), fxhour)
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
    p.setdpi(200)
    p.setmap(projection='lcc', georange=(10, 50, 75, 150), lat_1=30, lat_2=35, lat_0=35, lon_0=105)
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.25)
    c = p.contourf(t850, gpfcmap='temp2')
    p.contour(h500, levels=np.arange(4000, 6000, 40), clabeldict={'levels':np.arange(4000, 6000, 40)}, color='black',
              lw=0.3)
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF 850hPa Temperature, 500hPa Geopotential Height (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_t2m(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_T2M_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_HR/TMP_2M/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    lon, lat = np.meshgrid(x, y)
    lon = clip_data(x, y, lon, georange[2], georange[3], georange[0], georange[1])[::-1]
    lat = clip_data(x, y, lat, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.125)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    t2m = clip_data(x, y, t2m_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    c = p.contourf(t2m, gpfcmap='tt.850t')
    p.gridvalue(t2m, num=25, color='black')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF 2m Temperature (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_uv850_h500(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_UV850_H500_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
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
    h500 = clip_data(x, y, h500_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 10
    u850 = clip_data(x, y, u850_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 1.94
    v850 = clip_data(x, y, v850_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 1.94
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    c = p.contourf(h500, gpfcmap='geopo')
    lvl = [4600, 4660, 4720, 4780, 4840, 4900, 4960, 5020, 5060, 5100, 5140, 5180, 5220, 5260, 5300, 5340,
           5380, 5420, 5460, 5500, 5540, 5580, 5620, 5660, 5700, 5740, 5780, 5820, 5860, 5880, 5900, 5920,
           5940, 5970, 6000]
    p.contour(h500, levels=lvl, clabeldict={'levels':lvl}, color='black', lw=0.4, alpha=0.6)
    p.barbs(u850, v850, num=20)
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF 850hPa Wind Barbs, 500hPa Geopotential Height (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_asnow(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_ASNOW_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
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
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
    c = p.contourf(t2m_mdfs.data['Grid'][::-1], gpfcmap='wxb.snow')
    p.drawcoastline()
    p.drawprovinces()
    p.drawcities()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF Accumulated Total Precipitation (Snow) (mm) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_r24(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_R24_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_HR/RAIN24/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
    c = p.contourf(t2m_mdfs.data['Grid'][::-1], gpfcmap='wxb.rain')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF 24-Hour Precipitation (mm) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_r6(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_R06_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_HR/RAIN06/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
    c = p.contourf(t2m_mdfs.data['Grid'][::-1], gpfcmap='wxb.rain')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF 6-Hour Precipitation (mm) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_ptype(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    f_string = '{}_{}_EC_PTYPE.png'.format(lr.strftime('%Y%m%d%H'), fxhour)
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_HR/PRECIPITATION_TYPE/', fname)
    corr = await get_mdfs('ECMWF_HR/RAIN06/', fname)
    try:
        mdfs = MDFS_Grid(t2m_data)
        cor = MDFS_Grid(corr)
    except Exception:
        return None
    rain = cor.data['Grid'][::-1]
    x, y = mdfs.data['Lon'], mdfs.data['Lat']
    raw = mdfs.data['Grid'][::-1]
    raw[raw == 3] = 2
    raw[raw == 5] = 3
    raw[raw == 6] = 4
    raw[raw == 7] = 5
    raw[raw == 8] = 6
    raw = np.ma.array(raw, mask=np.logical_or(raw==0, rain==0))
    cmap = mcolor.ListedColormap(['#009AFF', '#9C00FF', '#00EB72', '#00F9F4', '#FF009C', '#630063'])
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=(17, 42, 90, 125))
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
    c = p.pcolormesh(raw, cmap=cmap, norm=mcolor.Normalize(1, 7))
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.title('ECMWF 3-Hour Precipitation Type (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    x1 = 0.9
    x2 = 0.11
    y1 = 0.02
    y2 = 0.3
    ax = p.fig.add_axes([x1, x2, y1, y2])
    cbar = p.fig.colorbar(c, cax=ax)
    cbar.ax.tick_params(labelsize=p.fontsize['cbar'])
    cbar.outline.set_linewidth(0.1)
    labels = np.arange(1, 7, 1)
    loc = labels + .5
    cbar.set_ticks(loc)
    cbar.ax.set_yticklabels(['Rain', 'Frz Rain', 'Dry Snow', 'Wet Snow', 'Sleet', 'Ice Pellet'])
    for l in cbar.ax.yaxis.get_ticklabels():
        l.set_family(p.family)
    cbar.ax.tick_params(axis='both', which='both',length=0)
    cbar.outline.set_visible(False)
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_cape(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_CAPE_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    cape_data = await get_mdfs('ECMWF_HR/CAPE/', fname)
    try:
        cape_mdfs = MDFS_Grid(cape_data)
    except Exception:
        return None
    #mdfs:///ECMWF_HR/UGRD_10M/
    x, y = cape_mdfs.data['Lon'], cape_mdfs.data['Lat']
    cape = clip_data(x, y, cape_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.125)
    c = p.contourf(cape, gpfcmap='wxb.cape')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF CAPE (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.maxminnote(cape, 'CAPE', 'J/kg')
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_uv925_tadv500(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_UV925_TADV500_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    u500_data = await get_mdfs('ECMWF_HR/UGRD/850/', fname)
    v500_data = await get_mdfs('ECMWF_HR/VGRD/850/', fname)
    u925_data = await get_mdfs('ECMWF_HR/UGRD/925/', fname)
    v925_data = await get_mdfs('ECMWF_HR/VGRD/925/', fname)
    t500_data = await get_mdfs('ECMWF_HR/TMP/500/', fname)
    try:
        u500_mdfs = MDFS_Grid(u500_data)
        v500_mdfs = MDFS_Grid(v500_data)
        u925_mdfs = MDFS_Grid(u925_data)
        v925_mdfs = MDFS_Grid(v925_data)
        t500_mdfs = MDFS_Grid(t500_data)
    except Exception:
        return None
    x, y = u500_mdfs.data['Lon'], u500_mdfs.data['Lat']
    dx, dy = mpcalc.lat_lon_grid_deltas(x[::-1], y[::-1])
    dy *= -1
    adv = mpcalc.advection(t500_mdfs.data['Grid'][::-1] * units.degC, [u500_mdfs.data['Grid'][::-1] * units('m/s'),
                           v500_mdfs.data['Grid'][::-1] * units('m/s')], (dx, dy))
    u925 = clip_data(x, y, u925_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 1.94
    v925 = clip_data(x, y, v925_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 1.94
    tadv = clip_data(x, y, adv, georange[2], georange[3], georange[0], georange[1])
    tadv = ndimage.gaussian_filter(tadv * 10e4, sigma=1, order=0)
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    c = p.contourf(tadv, gpfcmap='ncl.tdiff')
    p.barbs(u925, v925, num=20)
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF 925hPa Wind Barbs, 500hPa Temperature Advection (10^-4 K/s) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

def cal_tt(t850, td850, t500):
    return t850 + td850 - 2 * t500

def execute_sweat(tt, td8, u8, v8, u5, v5):
    s8 = np.sqrt(u8 * u8 + v8 * v8)
    s5 = np.sqrt(u5 * u5 + v5 * v5)
    s = s8 * s5
    z_mask = s == 0
    nz_mask = s != 0
    sdir = np.ndarray(tt.shape, np.float32)
    sdir[z_mask] = 0
    sdir[nz_mask] = (u5[nz_mask] * v8[nz_mask] - v5[nz_mask] * u8[nz_mask]) / s[nz_mask]
    tt49 = np.ndarray(tt.shape, np.float32)
    tt49_mask = tt >= 49
    tt49[tt < 49] = 0
    tt49[tt49_mask] = tt[tt49_mask] - 49.0
    result = 12 * td8
    result += 20 * tt49 
    result += 2 * 1.944 * s8
    result += s5 * 1.944
    result += 125 * (sdir + 0.2)
    return result

def c_sweat(t850, td850, t500, u850, v850, u500, v500):
    tt = cal_tt(t850, td850, t500)
    return execute_sweat(tt, td850, u850, v850, u500, v500)

async def ec_sweat(fxhour:str, **kw):
    #SWEAT=12*Td850 + 20*(TT-49) + 4*WF850 + 2*WF500 + 125*(sin(WD500-WD850)+0.2)
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_SWEAT_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    u850_data = await get_mdfs('ECMWF_HR/UGRD/850/', fname)
    v850_data = await get_mdfs('ECMWF_HR/VGRD/850/', fname)
    u500_data = await get_mdfs('ECMWF_HR/UGRD/850/', fname)
    v500_data = await get_mdfs('ECMWF_HR/VGRD/850/', fname)
    t850_data = await get_mdfs('ECMWF_HR/TMP/850/', fname)
    t500_data = await get_mdfs('ECMWF_HR/TMP/500/', fname)
    rh850_data = await get_mdfs('ECMWF_HR/RH/850/', fname)
    try:
        u850_mdfs = MDFS_Grid(u850_data)
        v850_mdfs = MDFS_Grid(v850_data)
        u500_mdfs = MDFS_Grid(u500_data)
        v500_mdfs = MDFS_Grid(v500_data)
        t850_mdfs = MDFS_Grid(t850_data)
        t500_mdfs = MDFS_Grid(t500_data)
        rh850_mdfs = MDFS_Grid(rh850_data)
    except Exception:
        return None
    td850 = mpcalc.dewpoint_rh(t850_mdfs.data['Grid'] * units.degC, rh850_mdfs.data['Grid'] * units.percent).magnitude
    sweat = c_sweat(t850_mdfs.data['Grid'], td850, t500_mdfs.data['Grid'], u850_mdfs.data['Grid'], v850_mdfs.data['Grid'],
                    u500_mdfs.data['Grid'], v500_mdfs.data['Grid'])
    sweat[sweat < 0] = 0
    x, y = t500_mdfs.data['Lon'], t500_mdfs.data['Lat']
    ret = clip_data(x, y, sweat, georange[2], georange[3], georange[0], georange[1])[::-1]
    u = clip_data(x, y, u850_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    v = clip_data(x, y, v850_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    c = p.contourf(ret, gpfcmap='sweat')
    p.streamplot(u, v, color='black', lw=0.3, arrowstyle='->', arrowsize=0.75)
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF SWEAT Index, 850hPa Streamlines (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.maxminnote(sweat[~np.isnan(sweat)], 'SWEAT', '')
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_kidx(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_KI_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t850_data = await get_mdfs('ECMWF_HR/TMP/850/', fname)
    t700_data = await get_mdfs('ECMWF_HR/TMP/700/', fname)
    t500_data = await get_mdfs('ECMWF_HR/TMP/500/', fname)
    rh850_data = await get_mdfs('ECMWF_HR/RH/850/', fname)
    rh700_data = await get_mdfs('ECMWF_HR/RH/700/', fname)
    try:
        t850_mdfs = MDFS_Grid(t850_data)
        t700_mdfs = MDFS_Grid(t700_data)
        t500_mdfs = MDFS_Grid(t500_data)
        rh850_mdfs = MDFS_Grid(rh850_data)
        rh700_mdfs = MDFS_Grid(rh700_data)
    except Exception:
        return None
    td850 = mpcalc.dewpoint_rh(t850_mdfs.data['Grid'] * units.degC, rh850_mdfs.data['Grid'] * units.percent).magnitude
    td700 = mpcalc.dewpoint_rh(t700_mdfs.data['Grid'] * units.degC, rh700_mdfs.data['Grid'] * units.percent).magnitude
    k = t850_mdfs.data['Grid'] - t500_mdfs.data['Grid'] + td850 - (t700_mdfs.data['Grid'] - td700)
    x, y = t500_mdfs.data['Lon'], t500_mdfs.data['Lat']
    ret = clip_data(x, y, k, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    c = p.contourf(ret, gpfcmap='wxb.kindex')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF K Index (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

#mdfs:///ECMWF_HR/PRMSL_UNCLIPPED/
async def ec_mslp(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_MSLP_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    mslp_data = await get_mdfs('ECMWF_HR/PRMSL_UNCLIPPED/', fname)
    try:
        mslp_mdfs = MDFS_Grid(mslp_data)
    except Exception:
        return None
    #mdfs:///ECMWF_HR/UGRD_10M/
    x, y = mslp_mdfs.data['Lon'], mslp_mdfs.data['Lat']
    mslp = clip_data(x, y, mslp_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    lon, lat = np.meshgrid(x, y)
    lon = clip_data(x, y, lon, georange[2], georange[3], georange[0], georange[1])[::-1]
    lat = clip_data(x, y, lat, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.125)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    c = p.contourf(mslp, gpfcmap='pres')
    p.maxminfilter(mslp, type='min', window=50, color='red')
    p.maxminfilter(mslp, type='max', window=50)
    #p.contour(mslp, levels=np.arange())
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF MSLP (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_thse850(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_THSE_850_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    rh850_data = await get_mdfs('ECMWF_HR/RH/850/', fname)
    t850_data = await get_mdfs('ECMWF_HR/TMP/850/', fname)
    try:
        rh850_mdfs = MDFS_Grid(rh850_data)
        t850_mdfs = MDFS_Grid(t850_data)
    except Exception:
        return None
    x, y = rh850_mdfs.data['Lon'], rh850_mdfs.data['Lat']
    p = 850 * np.ones(rh850_mdfs.data['Grid'].shape) * units('hPa')
    td850 = mpcalc.dewpoint_rh(t850_mdfs.data['Grid'] * units.degC, rh850_mdfs.data['Grid'] * units.percent)
    theta = mpcalc.equivalent_potential_temperature(p, t850_mdfs.data['Grid'] * units.degC, td850)
    ret = clip_data(x, y, theta, georange[2], georange[3], georange[0], georange[1])[::-1]
    lon, lat = np.meshgrid(x, y)
    lon = clip_data(x, y, lon, georange[2], georange[3], georange[0], georange[1])[::-1]
    lat = clip_data(x, y, lat, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    c = p.contourf(ret, gpfcmap='nmc.thse')
    p.contour(ret, levels=np.arange(312, 356, 4), alpha=0.6)
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF 850hPa Equivalent Potential Temperature (K) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_si(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_SI_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    rh850_data = await get_mdfs('ECMWF_HR/RH/850/', fname)
    t850_data = await get_mdfs('ECMWF_HR/TMP/850/', fname)
    t500_data = await get_mdfs('ECMWF_HR/TMP/500/', fname)
    try:
        rh850_mdfs = MDFS_Grid(rh850_data)
        t850_mdfs = MDFS_Grid(t850_data)
        t500_mdfs = MDFS_Grid(t500_data)
    except Exception:
        return None
    x, y = rh850_mdfs.data['Lon'], rh850_mdfs.data['Lat']
    td850 = mpcalc.dewpoint_rh(t850_mdfs.data['Grid'] * units.degC, rh850_mdfs.data['Grid'] * units.percent)
    si_vfunc = np.vectorize(SI_wrapper)
    si = si_vfunc(t850_mdfs.data['Grid'], td850.magnitude, t500_mdfs.data['Grid'])
    ret = clip_data(x, y, si, georange[2], georange[3], georange[0], georange[1])[::-1]
    lon, lat = np.meshgrid(x, y)
    lon = clip_data(x, y, lon, georange[2], georange[3], georange[0], georange[1])[::-1]
    lat = clip_data(x, y, lat, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    c = p.contourf(ret, gpfcmap='wxb.lifted')
    #p.contour(ret, levels=np.arange(312, 356, 4), alpha=0.6)
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF Showalter Index (Test product) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_li(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_LI_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    rh850_data = await get_mdfs('ECMWF_HR/RH/925/', fname)
    t850_data = await get_mdfs('ECMWF_HR/TMP/925/', fname)
    t500_data = await get_mdfs('ECMWF_HR/TMP/500/', fname)
    try:
        rh850_mdfs = MDFS_Grid(rh850_data)
        t850_mdfs = MDFS_Grid(t850_data)
        t500_mdfs = MDFS_Grid(t500_data)
    except Exception:
        return None
    x, y = rh850_mdfs.data['Lon'], rh850_mdfs.data['Lat']
    td850 = mpcalc.dewpoint_rh(t850_mdfs.data['Grid'] * units.degC, rh850_mdfs.data['Grid'] * units.percent)
    li_vfunc = np.vectorize(LI_wrapper)
    li = li_vfunc(t850_mdfs.data['Grid'], td850.magnitude, t500_mdfs.data['Grid'])
    ret = clip_data(x, y, li, georange[2], georange[3], georange[0], georange[1])[::-1]
    lon, lat = np.meshgrid(x, y)
    lon = clip_data(x, y, lon, georange[2], georange[3], georange[0], georange[1])[::-1]
    lat = clip_data(x, y, lat, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    c = p.contourf(ret, gpfcmap='wxb.lifted')
    #p.contour(ret, levels=np.arange(312, 356, 4), alpha=0.6)
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF Lifted Index (From 925hPa) (Test product) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_vv850(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_VV_850_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    cape_data = await get_mdfs('ECMWF_HR/VVEL/850/', fname)
    try:
        cape_mdfs = MDFS_Grid(cape_data)
    except Exception:
        return None
    #mdfs:///ECMWF_HR/UGRD_10M/
    x, y = cape_mdfs.data['Lon'], cape_mdfs.data['Lat']
    cape = clip_data(x, y, cape_mdfs.data['Grid'] * 0.1, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.25)
    c = p.contourf(cape, gpfcmap='ec.850t')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF Vertical Velocity (10^-1Pa/s) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def ec_lcl(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EC_LCL_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_HR/TMP_2M/', fname)
    td2m_data = await get_mdfs('ECMWF_HR/DPT_2M/', fname)
    stp_data = await get_mdfs('ECMWF_HR/PRES/SURFACE/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
        td2m_mdfs = MDFS_Grid(td2m_data)
        stp_mdfs = MDFS_Grid(stp_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    lon, lat = np.meshgrid(x, y)
    lon = clip_data(x, y, lon, georange[2], georange[3], georange[0], georange[1])[::-1]
    lat = clip_data(x, y, lat, georange[2], georange[3], georange[0], georange[1])[::-1]
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.125)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    t2m = clip_data(x, y, t2m_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    psfc = clip_data(x, y, stp_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    td = clip_data(x, y, td2m_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
    plcl, tlcl = mpcalc.lcl(psfc * units('hPa'), t2m * units.degC, td *units.degC)
    h1 = mpcalc.pressure_to_height_std(psfc * units('hPa'))
    h2 = mpcalc.pressure_to_height_std(plcl)
    hlcl = h2 - h1
    c = p.contourf(hlcl.magnitude * 1000, gpfcmap='lcl')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('ECMWF Lifted Condensation Level (AGL) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

#mdfs:///ECMWF_HR/VVEL/850/

FUNC_CONV = {'T2M':ec_t2m, 'T850H500':ec_t850_h500, 'UV850H500':ec_uv850_h500, 'ASNOW':ec_asnow,
             'R24':ec_r24, 'PTYPE':ec_ptype, 'CAPE':ec_cape, 'SWEAT':ec_sweat, 'K':ec_kidx,
             'MSLP':ec_mslp, 'R06':ec_r6, 'THSE850':ec_thse850, 'SI':ec_si, 'LI':ec_li, 'VV850':ec_vv850,
             'LCL':ec_lcl}

@on_command('EC', only_to_me=False)
async def call_ec_func(session:CommandSession):
    ids = get_id(session)
    raw = session.ctx['raw_message'].split('EC')[1].strip()
    command = raw.split(' ')
    logger.info(command)
    if ids not in PERMITUSERS and command[0] != 'SI':
        await session.send('此功能为付费功能，请付费后调用')
        raise PermissionError('Permission denied')
    func = FUNC_CONV[command[0]]
    try:
        fx = int(command[1])
    except Exception as e:
        await session.send(e)
    fs = convert_time(fx)
    if len(command) > 2:
        shell = parse_shell_command(command[2])
    else:
        shell = {}
    try:
        fpath = await func(fs, **shell)
    except Exception as e:
        import traceback
        await session.send(traceback.format_exc())
    if fpath:
        await session.send('[CQ:image,file={}]'.format(fpath))
    else:
        await session.send('当前预报时效无数据')
    plt.close('all')

async def sh_cr(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_SHHR_CR_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('SHANGHAI_HR/COMPOSITE_REFLECTIVITY/ENTIRE_ATMOSPHERE/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    lon, lat = np.meshgrid(x, y)
    lon = clip_data_sh(x, y, lon, georange[2], georange[3], georange[0], georange[1])
    lat = clip_data_sh(x, y, lat, georange[2], georange[3], georange[0], georange[1])
    cr = clip_data_sh(x, y, t2m_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.1)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    c = p.contourf(cr, gpfcmap='nmc.ref')
    p.drawcoastline()
    p.drawprovinces()
    p.colorbar(c)
    p.title('Shanghai HRES Composite Reflectivity (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def sh_sweat(fxhour:str, **kw):
    #SWEAT=12*Td850 + 20*(TT-49) + 4*WF850 + 2*WF500 + 125*(sin(WD500-WD850)+0.2)
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_SHHR_SWEAT_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    u850_data = await get_mdfs('SHANGHAI_HR/UGRD/850/', fname)
    v850_data = await get_mdfs('SHANGHAI_HR/VGRD/850/', fname)
    u500_data = await get_mdfs('SHANGHAI_HR/UGRD/850/', fname)
    v500_data = await get_mdfs('SHANGHAI_HR/VGRD/850/', fname)
    t850_data = await get_mdfs('SHANGHAI_HR/TMP/850/', fname)
    t500_data = await get_mdfs('SHANGHAI_HR/TMP/500/', fname)
    rh850_data = await get_mdfs('SHANGHAI_HR/RH/850/', fname)
    try:
        u850_mdfs = MDFS_Grid(u850_data)
        v850_mdfs = MDFS_Grid(v850_data)
        u500_mdfs = MDFS_Grid(u500_data)
        v500_mdfs = MDFS_Grid(v500_data)
        t850_mdfs = MDFS_Grid(t850_data)
        t500_mdfs = MDFS_Grid(t500_data)
        rh850_mdfs = MDFS_Grid(rh850_data)
    except Exception:
        return None
    x, y = u850_mdfs.data['Lon'], u850_mdfs.data['Lat']
    td850 = mpcalc.dewpoint_rh(t850_mdfs.data['Grid'] * units.degC, rh850_mdfs.data['Grid'] * units.percent).magnitude
    sweat = c_sweat(t850_mdfs.data['Grid'], td850, t500_mdfs.data['Grid'], u850_mdfs.data['Grid'], v850_mdfs.data['Grid'],
                    u500_mdfs.data['Grid'], v500_mdfs.data['Grid'])
    sweat[sweat < 0] = 0
    ret = clip_data_sh(x, y, sweat, georange[2], georange[3], georange[0], georange[1])
    x, y = t500_mdfs.data['Lon'], t500_mdfs.data['Lat']
    lon, lat = np.meshgrid(x, y)
    lon = clip_data_sh(x, y, lon, georange[2], georange[3], georange[0], georange[1])
    lat = clip_data_sh(x, y, lat, georange[2], georange[3], georange[0], georange[1])
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.1)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    c = p.contourf(ret, gpfcmap='sweat')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('Shanghai HRES SWEAT Index (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

async def sh_r1(fxhour:str, **kw):
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    logger.info(kw)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_SHHR_R01_'.format(lr.strftime('%Y%m%d%H'), fxhour)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('SHANGHAI_HR/RAIN01/', fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    lon, lat = np.meshgrid(x, y)
    ret = clip_data_sh(x, y, t2m_mdfs.data['Grid'], georange[2], georange[3], georange[0], georange[1])
    lon = clip_data_sh(x, y, lon, georange[2], georange[3], georange[0], georange[1])
    lat = clip_data_sh(x, y, lat, georange[2], georange[3], georange[0], georange[1])
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy(georange, 0.1)
    p.x = lon[0]
    p.y = lat[:, 0]
    p.xx = lon
    p.yy = lat
    c = p.contourf(ret, gpfcmap='wxb.rain1')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('Shanghai HRES 1-Hour Precipitation (mm) (Generated by QQbot)', nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

FUNC_CONV_SH = {'CR':sh_cr, 'SWEAT':sh_sweat, 'R01':sh_r1}

@on_command('SHHR', only_to_me=False)
async def call_sh_func(session:CommandSession):
    ids = get_id(session)
    if ids not in PERMITUSERS:
        await session.send('此功能为付费功能，请付费后调用')
        raise PermissionError('Permission denied')
    raw = session.ctx['raw_message'].split('SHHR')[1].strip()
    command = raw.split(' ')
    func = FUNC_CONV_SH[command[0]]
    try:
        fx = int(command[1])
    except Exception as e:
        await session.send(e)
    fs = convert_time(fx)
    if len(command) > 2:
        shell = parse_shell_command(command[2])
    else:
        shell = {}
    fpath = await func(fs, **shell)
    if fpath:
        await session.send('[CQ:image,file={}]'.format(fpath))
    else:
        await session.send('当前预报时效无数据')
    plt.close('all')

async def eps_r24_per(fxhour:str, percentile, **kw):
    # Convert percentile
    #mdfs:///ECMWF_ENSEMBLE_PRODUCT/STATISTICS/0.0/RAIN24/
    p = percentile / 100
    if percentile == 0:
        p = '0.0'
    lr = get_latest_run()
    init_time = lr + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
    logger.info(kw)
    georange = kw.pop('georange', None)
    if not georange:
        georange = (17, 42, 90, 125)
    f_string = '{}_{}_EPS_R01_{}_'.format(lr.strftime('%Y%m%d%H'), fxhour, percentile)
    f_string += '_'.join([str(i) for i in georange]) + '.png'
    fpath = pic_root.joinpath('mpkit', f_string).as_posix()
    if os.path.exists(fpath):
        return 'mpkit/' + f_string
    t2m_data = await get_mdfs('ECMWF_ENSEMBLE_PRODUCT/STATISTICS/{}/RAIN24/'.format(p), fname)
    try:
        t2m_mdfs = MDFS_Grid(t2m_data)
    except Exception:
        return None
    x, y = t2m_mdfs.data['Lon'], t2m_mdfs.data['Lat']
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(200)
    p.setmap(georange=georange)
    p.setxy((y.min(), y.max(), x.min(), x.max()), 0.5)
    c = p.contourf(t2m_mdfs.data['Grid'][::-1], gpfcmap='wxb.rain')
    p.drawcoastline()
    p.drawprovinces()
    p.drawparameri(lw=0)
    p.colorbar(c)
    p.title('EPS {}% Percentile 24-Hour Precipitation (mm) (Generated by QQbot)'.format(percentile), nasdaq=False)
    p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
    p.save(fpath)
    return 'mpkit/' + f_string

FUNC_CONV_EPS = {'R24':eps_r24_per}

@on_command('EPS', only_to_me=False)
async def call_eps_func(session:CommandSession):
    ids = get_id(session)
    if ids not in PERMITUSERS:
        await session.send('此功能为付费功能，请付费后调用')
        raise PermissionError('Permission denied')
    raw = session.ctx['raw_message'].split('EPS')[1].strip()
    command = raw.split(' ')
    c0 = command[0]
    c_group = c0.split('-')
    func = FUNC_CONV_EPS[c_group[0]]
    try:
        fx = int(command[1])
    except Exception as e:
        await session.send(e)
    fs = convert_time(fx)
    if len(command) > 2:
        shell = parse_shell_command(command[2])
    else:
        shell = {}
    fpath = await func(fs, int(c_group[1]), **shell)
    if fpath:
        await session.send('[CQ:image,file={}]'.format(fpath))
    else:
        await session.send('当前预报时效无数据')
    plt.close('all')
