import os
from pathlib import Path
from io import BytesIO
import struct
from ctypes import *

from nonebot import on_command, CommandSession, permission, Scheduler
from nonebot.log import logger
import requests
import datetime
import numpy as np
import matplotlib.colors as mcolor
import matplotlib.pyplot as plt
import metpy.calc as mpcalc
from metpy.units import units

from . import DataBlock_pb2
from .plotplus import Plot
from .permit import get_perm_usr
from .utils import args_parser

dll = CDLL('SI_DLL.dll')
dll.showalter_index.restype = c_double
dll.lifted_index.restype = c_double

def SI_wrapper(t850, td850, t500):
    return dll.showalter_index(c_double(t850), c_double(td850), c_double(t500))

def LI_wrapper(t850, td850, t500):
    return dll.lifted_index(c_double(t850), c_double(td850), c_double(t500))

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
    if not os.path.exists(base):
        _bytearray = DataBlock_pb2.ByteArrayResult()
        url = 'http://10.116.32.66:8080/DataService?requestType=getData&directory={}&fileName={}'.format(directory, filename)
        logger.info('Downloading {}'.format(url))
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

class process(object):
    def __init__(self, pic_name, prod_path):
        self.pic_name = pic_name
        self.prod_path = prod_path

    def __call__(self, func):
        async def deco(*args, **kwargs):
            fxhour = args[0]
            lr = get_latest_run()
            init_time = lr + datetime.timedelta(hours=8)
            fname = init_time.strftime('%y%m%d%H') + '.{}'.format(fxhour)
            georange = kwargs.pop('georange', None)
            if not georange:
                georange = (17, 42, 90, 125)
            kwargs['georange'] = georange
            f_string = '{}_{}_{}'.format(lr.strftime('%Y%m%d%H'), fxhour, self.pic_name)
            f_string += '_'.join([str(i) for i in georange]) + '.png'
            fpath = pic_root.joinpath('mpkit', f_string).as_posix()
            kwargs['time'] = lr
            kwargs['fp'] = fpath
            kwargs['fs'] = f_string
            if os.path.exists(fpath):
                data = []
            else:
                if kwargs.pop('unclip', None):
                    addi = '_UNCLIPPED'
                else:
                    addi = ''
                data = list()
                prod = (self.prod_path,) if isinstance(self.prod_path, str) else self.prod_path
                for i in prod:
                    if not i.endswith('{}/'):
                        i = i[:-1] + '{}/'
                    tmp = await get_mdfs(i.format(addi), fname)
                    try:
                        data.append(MDFS_Grid(tmp))
                    except Exception:
                        data = None
                        break
            return await func(*args, data=data, **kwargs)
        return deco

@process('EC_T850_H500', ['ECMWF_HR/TMP/850/', 'ECMWF_HR/HGT/500/'])
async def ec_t850_h500(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            t850 = data[0].data['Grid'][::-1]
            h500 = data[1].data['Grid'][::-1] * 10
            x, y = data[0].data['Lon'], data[0].data['Lat']
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
    else:
        return None

@process('EC_T2M', ['ECMWF_HR/TMP_2M/'])
async def ec_t2m(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
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
            t2m = clip_data(x, y, data[0].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
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
    else:
        return None

@process('EC_UV850_H500', ['ECMWF_HR/HGT/500/', 'ECMWF_HR/UGRD/850/', 'ECMWF_HR/VGRD/850/'])
async def ec_uv850_h500(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            h500 = clip_data(x, y, data[0].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 10
            u850 = clip_data(x, y, data[1].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 1.94
            v850 = clip_data(x, y, data[2].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1] * 1.94
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
    else:
        return None

@process('EC_ASNOW', ['ECMWF_HR/ASNOW/'])
async def ec_asnow(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            p = Plot()
            p.setfamily('Arial')
            p.setdpi(200)
            p.setmap(georange=georange)
            p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
            c = p.contourf(data[0].data['Grid'][::-1], gpfcmap='wxb.snow')
            p.drawcoastline()
            p.drawprovinces()
            p.drawcities()
            p.drawparameri(lw=0)
            p.colorbar(c)
            p.title('ECMWF Accumulated Total Precipitation (Snow) (mm) (Generated by QQbot)', nasdaq=False)
            p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
            p.save(fpath)
        return 'mpkit/' + f_string
    else:
        return None

@process('EC_R24', ['ECMWF_HR/RAIN24/'])
async def ec_r24(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            p = Plot()
            p.setfamily('Arial')
            p.setdpi(200)
            p.setmap(georange=georange)
            p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
            c = p.contourf(data[0].data['Grid'][::-1], gpfcmap='wxb.rain')
            p.drawcoastline()
            p.drawprovinces()
            p.drawparameri(lw=0)
            p.colorbar(c)
            p.title('ECMWF 24-Hour Precipitation (mm) (Generated by QQbot)', nasdaq=False)
            p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
            p.save(fpath)
        return 'mpkit/' + f_string
    else:
        return None

@process('EC_R06', ['ECMWF_HR/RAIN06/'])
async def ec_r6(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            p = Plot()
            p.setfamily('Arial')
            p.setdpi(200)
            p.setmap(georange=georange)
            p.setxy((y.min(), y.max(), x.min(), x.max()), 0.125)
            c = p.contourf(data[0].data['Grid'][::-1], gpfcmap='wxb.rain')
            p.drawcoastline()
            p.drawprovinces()
            p.drawparameri(lw=0)
            p.colorbar(c)
            p.title('ECMWF 6-Hour Precipitation (mm) (Generated by QQbot)', nasdaq=False)
            p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
            p.save(fpath)
        return 'mpkit/' + f_string
    else:
        return None

@process('EC_PTYPE', ['ECMWF_HR/PRECIPITATION_TYPE/', 'ECMWF_HR/RAIN06/'])
async def ec_ptype(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            rain = data[1].data['Grid'][::-1]
            x, y = data[0].data['Lon'], data[0].data['Lat']
            raw = data[0].data['Grid'][::-1]
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
    else:
        return None

@process('EC_CAPE', ['ECMWF_HR/CAPE/'])
async def ec_cape(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            cape = clip_data(x, y, data[0].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
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
    else:
        return None

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

@process('EC_SWEAT', ['ECMWF_HR/UGRD/850/', 'ECMWF_HR/VGRD/850/', 'ECMWF_HR/UGRD/500/',
                      'ECMWF_HR/VGRD/500/', 'ECMWF_HR/TMP/850/', 'ECMWF_HR/TMP/500/', 'ECMWF_HR/RH/850/'])
async def ec_sweat(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            td850 = mpcalc.dewpoint_rh(data[4].data['Grid'] * units.degC, data[6].data['Grid'] * units.percent).magnitude
            sweat = c_sweat(data[4].data['Grid'], td850, data[5].data['Grid'], data[0].data['Grid'], data[1].data['Grid'],
                            data[2].data['Grid'], data[3].data['Grid'])
            sweat[sweat < 0] = 0
            x, y = data[0].data['Lon'], data[0].data['Lat']
            ret = clip_data(x, y, sweat, georange[2], georange[3], georange[0], georange[1])[::-1]
            u = clip_data(x, y, data[0].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
            v = clip_data(x, y, data[1].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
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
    else:
        return None

@process('EC_KI', ['ECMWF_HR/TMP/850/', 'ECMWF_HR/TMP/700/', 'ECMWF_HR/TMP/500/', 'ECMWF_HR/RH/850/',
                   'ECMWF_HR/RH/700/'])
async def ec_kidx(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            td850 = mpcalc.dewpoint_rh(data[0].data['Grid'] * units.degC, data[3].data['Grid'] * units.percent).magnitude
            td700 = mpcalc.dewpoint_rh(data[1].data['Grid'] * units.degC, data[4].data['Grid'] * units.percent).magnitude
            k = data[0].data['Grid'] - data[2].data['Grid'] + td850 - (data[1].data['Grid'] - td700)
            x, y = data[2].data['Lon'], data[2].data['Lat']
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
    else:
        return None

@process('EC_MSLP', ['ECMWF_HR/PRMSL/'])
async def ec_mslp(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            mslp = clip_data(x, y, data[0].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
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
            p.maxminfilter(mslp, type='min', window=500, color='red', marktext=True, marktextdict=dict(mark='L', markfontsize=6))
            p.maxminfilter(mslp, type='max', window=500, color='blue', marktext=True, marktextdict=dict(mark='H', markfontsize=6))
            #p.contour(mslp, levels=np.arange())
            p.drawcoastline()
            p.drawprovinces()
            p.drawparameri(lw=0)
            p.colorbar(c)
            p.title('ECMWF MSLP (Generated by QQbot)', nasdaq=False)
            p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
            p.save(fpath)
        return 'mpkit/' + f_string
    else:
        return None

@process('EC_THSE_850', ['ECMWF_HR/RH/850/', 'ECMWF_HR/TMP/850/'])
async def ec_thse850(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            p = 850 * np.ones(data[0].data['Grid'].shape) * units('hPa')
            td850 = mpcalc.dewpoint_rh(data[1].data['Grid'] * units.degC, data[0].data['Grid'] * units.percent)
            theta = mpcalc.equivalent_potential_temperature(p, data[1].data['Grid'] * units.degC, td850)
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
    else:
        return None

@process('EC_SI', ['ECMWF_HR/RH/850/', 'ECMWF_HR/TMP/850/', 'ECMWF_HR/TMP/500/'])
async def ec_si(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            td850 = mpcalc.dewpoint_rh(data[1].data['Grid'] * units.degC, data[0].data['Grid'] * units.percent)
            si_vfunc = np.vectorize(SI_wrapper)
            si = si_vfunc(data[1].data['Grid'], td850.magnitude, data[2].data['Grid'])
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
    else:
        return None

@process('EC_VV_850', ['ECMWF_HR/VVEL/850/'])
async def ec_vv850(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
            cape = clip_data(x, y, data[0].data['Grid'] * 0.1, georange[2], georange[3], georange[0], georange[1])[::-1]
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
            p.title('ECMWF 850hPa Vertical Velocity (10^-1Pa/s) (Generated by QQbot)', nasdaq=False)
            p.timestamp(lr.strftime('%Y%m%d%H'), int(fxhour))
            p.save(fpath)
        return 'mpkit/' + f_string
    else:
        return None

@process('EC_LCL', ['ECMWF_HR/TMP_2M/', 'ECMWF_HR/DPT_2M/', 'ECMWF_HR/PRES/SURFACE/'])
async def ec_lcl(fxhour:str, data=None, **kw):
    if isinstance(data, list):
        f_string = kw.pop('fs')
        if data:
            lr = kw.pop('time')
            fpath = kw.pop('fp')
            georange = kw.pop('georange')
            x, y = data[0].data['Lon'], data[0].data['Lat']
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
            t2m = clip_data(x, y, data[0].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
            psfc = clip_data(x, y, data[2].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
            td = clip_data(x, y, data[1].data['Grid'], georange[2], georange[3], georange[0], georange[1])[::-1]
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
    else:
        return None

#mdfs:///ECMWF_HR/VVEL/850/

FUNC_CONV = {'T2M':ec_t2m, 'T850H500':ec_t850_h500, 'UV850H500':ec_uv850_h500, 'ASNOW':ec_asnow,
             'R24':ec_r24, 'PTYPE':ec_ptype, 'CAPE':ec_cape, 'SWEAT':ec_sweat, 'K':ec_kidx,
             'MSLP':ec_mslp, 'R06':ec_r6, 'THSE850':ec_thse850, 'SI':ec_si, 'VV850':ec_vv850,
             'LCL':ec_lcl}

@on_command('EC', only_to_me=False)
async def call_ec_func(session:CommandSession):
    PERMITUSERS = get_perm_usr()
    ids = get_id(session)
    raw = session.ctx['raw_message'].split('EC')[1].strip()
    command = raw.split(' ')
    logger.info(command)
    #if ids not in PERMITUSERS and command[0] != 'SI':
    #    await session.send('此功能为付费功能，请付费后调用')
    #    raise PermissionError('Permission denied')
    func = FUNC_CONV[command[0]]
    try:
        fx = int(command[1])
    except Exception as e:
        await session.send(e)
    fs = convert_time(fx)
    if len(command) > 2:
        args = command[2:]
        shell = args_parser(' '.join(args))
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
    PERMITUSERS = get_perm_usr()
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
        args = command[2:]
        shell = args_parser(' '.join(args))
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
    PERMITUSERS = get_perm_usr()
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
        args = command[2:]
        shell = args_parser(' '.join(args))
    else:
        shell = {}
    fpath = await func(fs, int(c_group[1]), **shell)
    if fpath:
        await session.send('[CQ:image,file={}]'.format(fpath))
    else:
        await session.send('当前预报时效无数据')
    plt.close('all')
