from nonebot import on_command, CommandSession, logger
import requests
import time
import datetime
import numpy as np
import matplotlib as mpl
mpl.rc('font', family='Arial')
import matplotlib.pyplot as plt
from metpy import calc as mc
from metpy.units import units
import pickle

from .database import DBRecord
from .permit import get_perm_usr
from .autopic import get_id

with open('station_reverse.pickle', 'rb') as buf:
    index = pickle.load(buf)
with open('station.pickle', 'rb') as buf:
    cor = pickle.load(buf)

def plot_var(stid:str):
    if not stid.isnumeric():
        stn = stid
        stid = index[stid]
    else:
        try:
            stn = cor[stid]
        except KeyError:
            stn = stid
    req = requests.get('http://10.1.64.146/npt/livemonitor/recordsbyid?stationid={}'.format(stid))
    js = req.json()

    time_arr = [time.localtime(i[0] / 1000) for i in js[0]['data']]
    dtime_arr = [datetime.datetime(*i[:6]) - datetime.timedelta(hours=8) for i in time_arr]
    t = np.array([i[1] for i in js[0]['data']], float)
    t = np.ma.masked_invalid(t)
    rh = np.array([i[1] for i in js[3]['data']], float)
    rh = np.ma.masked_invalid(rh)
    dt = mc.dewpoint_rh(t * units.degC, rh * units.percent)
    hi = mc.heat_index(t * units.degC, rh * units.percent).to(units.degC)

    plt.figure(figsize=(10, 6))
    plt.grid()
    ax1 = plt.gca()
    ax1.plot(dtime_arr[-144:], t[-144:], color='red', label='Temperature')
    ax1.plot(dtime_arr[-144:], dt[-144:].magnitude, color='green', label='Dew point')
    plt.title('AWS Temperature @HCl\nStation: {}'.format(stid), loc='left')
    plt.legend(framealpha=0.5, loc='upper left')
    if ~hi.mask[-144:].all():
        ax2 = plt.twinx()
        ax2.plot(dtime_arr[-144:], hi[-144:].magnitude, color='orange', label='Heat Index')
        plt.legend(framealpha=0.5, loc='upper right')
    maxp = np.nanargmax(t[-144:])
    minp = np.nanargmin(t[-144:])
    lim = ax1.get_ylim()
    rg = lim[1] - lim[0]
    ax1.annotate(str(np.nanmax(t[-144:])), xy=(dtime_arr[-144:][maxp], t[-144:][maxp]), xytext=(dtime_arr[-144:][maxp], t[-144:][maxp] - rg * 0.1),
                       arrowprops={'arrowstyle':'->'})
    ax1.annotate(str(np.nanmin(t[-144:])), xy=(dtime_arr[-144:][minp], t[-144:][minp]), xytext=(dtime_arr[-144:][minp], t[-144:][minp] + rg * 0.1),
                       arrowprops={'arrowstyle':'->'})
    plt.xlim(dtime_arr[-144], dtime_arr[-1])
    plt.savefig(r'D:\酷Q Pro\data\image\aws\{}_{}.png'.format(stid, dtime_arr[-1].strftime('%Y%m%d%H%M%S')),
                bbox_inches='tight')
    plt.close('all')
    return 'aws\{}_{}.png'.format(stid, dtime_arr[-1].strftime('%Y%m%d%H%M%S'))

@on_command('AWST', only_to_me=False)
async def ses(session:CommandSession):
    ids = get_id(session)
    db = DBRecord()
    ctx = session.ctx
    raw = ctx['raw_message'].split('AWST')[1].strip()
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
    PERMITUSER = get_perm_usr()
    if db.is_aws_exceed(ids) and (ids not in PERMITUSER):
        await session.send('今日免费调用次数已用完，付费后可继续调用')
        raise PermissionError('')
    db.aws(ids, raw)
    try:
        picp = plot_var(raw)
        await session.send('[CQ:image,file={}]'.format(picp))
    except Exception:
        import traceback
        await session.send(traceback.format_exc())