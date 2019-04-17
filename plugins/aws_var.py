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

from .database import DBRecord
from .permit import get_perm_usr
from .autopic import get_id

def plot_var(stid:str):
    req = requests.get('http://10.1.64.146/npt/livemonitor/recordsbyid?stationid={}'.format(stid))
    js = req.json()

    time_arr = [time.localtime(i[0] / 1000) for i in js[0]['data']]
    dtime_arr = [datetime.datetime(*i[:6]) - datetime.timedelta(hours=8) for i in time_arr]
    t = np.array([i[1] for i in js[0]['data']], float)
    rh = np.array([i[1] for i in js[3]['data']], float)
    dt = mc.dewpoint_rh(t * units.degC, rh * units.percent)
    plt.figure(figsize=(10, 6))
    plt.grid()
    plt.plot(dtime_arr[-144:], t[-144:], color='red', label='Temperature')
    plt.plot(dtime_arr[-144:], dt[-144:].magnitude, color='green', label='Dew point')
    plt.title('AWS Temperature @HCl\nStation: {}'.format(stid), loc='left')
    maxp = np.nanargmax(t[-144:])
    minp = np.nanargmin(t[-144:])
    lim = plt.gca().get_ylim()
    rg = lim[1] - lim[0]
    plt.gca().annotate(str(np.nanmax(t[-144:])), xy=(dtime_arr[-144:][maxp], t[-144:][maxp]), xytext=(dtime_arr[-144:][maxp], t[-144:][maxp] - rg * 0.1),
                       arrowprops={'arrowstyle':'->'})
    plt.gca().annotate(str(np.nanmin(t[-144:])), xy=(dtime_arr[-144:][minp], t[-144:][minp]), xytext=(dtime_arr[-144:][minp], t[-144:][minp] + rg * 0.1),
                       arrowprops={'arrowstyle':'->'})
    plt.xlim(dtime_arr[-144], dtime_arr[-1])
    plt.legend(framealpha=0.5)
    plt.savefig(r'D:\酷Q Pro\data\image\aws\{}_{}.png'.format(stid, dtime_arr[-1].strftime('%Y%m%d%H%M%S')),
                bbox_inches='tight')
    plt.close('all')
    return 'aws\{}_{}.png'.format(stid, dtime_arr[-1].strftime('%Y%m%d%H%M%S'))

@on_command('AWST', only_to_me=False)
async def ses(session:CommandSession):
    ids = get_id(session)
    PERMITUSER = get_perm_usr()
    db = DBRecord()
    if db.is_aws_exceed(ids) and (ids not in PERMITUSER):
        await session.send('今日免费调用次数已用完，付费后可继续调用')
        raise PermissionError('')
    raw = session.ctx['raw_message'].split('AWST')[1].strip()
    db.aws(ids, raw)
    try:
        picp = plot_var(raw)
        await session.send('[CQ:image,file={}]'.format(picp))
    except Exception:
        import traceback
        await session.send(traceback.format_exc())