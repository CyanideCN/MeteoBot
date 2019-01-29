import os
from pathlib import Path
from io import BytesIO

from nonebot import on_command, CommandSession, permission, Scheduler
#from nonebot.scheduler import scheduled_job
from nonebot.log import logger
import requests
import datetime

from . import DataBlock_pb2
from .plotplus import Plot

def get_latest_run():
    now = datetime.datetime.utcnow()
    if now.hour > 7 or now.hour < 18:
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
'''
tt_pic_count = 1
@scheduled_job('cron', hour='2-4', minute='*/6')
async def tt_ec_auto_uploader(session:CommandSession):
    global tt_pic_count
    t = get_latest_run()
'''

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
    #https://www.tropicaltidbits.com/analysis/models/ecmwf/2019012900/ecmwf_T850_ea_1.png
#http://10.116.32.66:8080/DataService?requestType=getData&directory=ECMWF_HR/PRECIPITATION_TYPE/&fileName=19012720.084

async def get_mdfs(directory, filename):
    _bytearray = DataBlock_pb2.ByteArrayResult()
    url = 'http://10.116.32.66:8080/DataService?requestType=getData&directory={}&fileName={}'.format(directory, filename)
    req = requests.get(url)
    _bytearray.ParseFromString(req.content)
    return MDFS_Grid(BytesIO(_bytearray.byteArray))

async def ec_t850_h500(fxhour:str):
    init_time = get_latest_run() + datetime.timedelta(hours=8)
    fname = init_time.strftime('%y%m%d%h') + '.{}'.format(fxhour)
    f_string = '{}_{}_EC_T850_H500.png'.format(init_time.strftime('%Y%m%d%h'), fxhour)
    fpath = pic_root.joinpath(f_string).as_posix()
    if os.path.exists(fpath):
        return
    t850_mdfs = await get_mdfs('ECMWF_HR/TMP/850/', fname)
    h500_mdfs = await get_mdfs('ECMWF_HR/HGT/500/', fname)
    t850 = t850_mdfs.data['Grid']
    h500 = h500_mdfs.data['Grid']
    georange = (0, 80, 50, 170)
    p = Plot()
    p.setfamily('Arial')
    p.setdpi(300)
    p.setmap(projection='lcc', georange=(10, 50, 75, 150), lat_1=30, lat_2=35, lat_0=35, lon_0=105)
    p.setxy(georange, 0.5)
    c = p.contourf(t, gpfcmap='temp2')
    p.contour(h500, levels=np.arange(400, 600, 4), clabeldict={'levels':np.arange(400, 600, 4)}, color='black',
              lw=0.3)
    p.contour(h500, levels=588, color='red')
    #p.barbs(u, v, color='black')
    p.drawcoastline()
    p.drawprovinces()
    p.colorbar(c)
    #p.maxminnote(t, 'Tempearture', 'deg')
    #p.gridvalue(t)
    p.title('ECMWF 850hPa Temperature, 500hPa Geopotential Height', nasdaq=False)
    p.timestamp(init_time.strftime('%Y%m%d%h'), int(fxhour))
    p.save(fpath)
    return fpath

@on_command('ECT850H500')
async def on_send_ec_t850_h500(session:CommandSession):
    fxh = session.get('fxh')
    try:
        fx = int(fxh)
    except Exception as e:
        await session.send(e)
    if fx in range(0, 10):
        fs = '00{}'.format(fx)
    elif fx in range(10, 100):
        fs = '0{}'.format(fx)
    else:
        fs = str(fx)
    fpath = await ec_t850_h500(fs)
    await session.send('[CQ:image,file={}]'.format(fpath))

@on_send_ec_t850_h500.args_parser
async def parse_ec_t850_h500(session:CommandSession):
    stripped_arg = session.current_arg_text.strip()
    if session.current_key:
        session.args[session.current_key] = stripped_arg
    elif stripped_arg:
        session.args['fxh'] = stripped_arg