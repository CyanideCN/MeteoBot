#coding = utf-8
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import metpy.calc as mpcalc
from metpy.plots import SkewT, Hodograph
from metpy.units import units
import metpy.interpolate as mpi
import datetime
import requests
import pathlib
import os
from nonebot import on_command, CommandSession, logger
from .autopic import get_id, PERMITUSERS

def download():
    now = datetime.datetime.now()
    if now.time() < datetime.time(20, 45) and now.time() > datetime.time(8, 45):
        fp = now.strftime('%Y%m%d') + '080000.000'
    elif now.time() > datetime.time(20, 45):
        fp = now.strftime('%Y%m%d') + '200000.000'
    else:
        fp = (now - datetime.timedelta(days=1)).strftime('%Y%m%d') + '200000.000'
    root = pathlib.Path(r'D:\Meteorology\MDFS\UPPER_AIR\TLOGP')
    pth = root.joinpath(fp)
    if not os.path.exists(pth):
        url = 'http://10.116.32.66:8080/DataService?requestType=getData&directory={}&fileName={}'.format('UPPER_AIR/TLOGP/', fp)
        logger.info(url)
        req = requests.get(url)
        ctt = req.content[13:].decode()
        f = open(pth, 'w', newline='')
        f.write(ctt)
        f.close()
    f = open(pth, 'r')
    return f

def get_data_section(buffer, station_id):
    buffer.seek(0)
    content = buffer.read()
    try:
        pos = content.index(station_id)
    except Exception as e:
        return None, None
    buffer.seek(pos)
    out = list()
    info = buffer.readline()
    while 1:
        line = buffer.readline()
        if line.startswith(' '):
            out.append(line.strip().split('  '))
        else:
            buffer.close()
            return info.strip().split(' '), out

def get_pressure_level_index(p, plevel, reverse=False):
    if reverse:
        idx = 0
    else:
        idx = -1
    return np.where(p.magnitude>=plevel)[0][idx]

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

def showalter_index(t850, td850, t500):
    plcl, tlcl = mpcalc.lcl(850 * units('hPa'), t850, td850)
    p = np.array([plcl.magnitude, 500]) * units('hPa')
    out = mpcalc.moist_lapse(p, tlcl)
    return t500.magnitude - out[1].magnitude

def lifted_index(tsfc, tdsfc, psfc, t500):
    plcl, tlcl = mpcalc.lcl(psfc, tsfc, tdsfc)
    p = np.array([plcl.magnitude, 500]) * units('hPa')
    out = mpcalc.moist_lapse(p, tlcl)
    return t500.magnitude - out[1].magnitude

def tlogp(stid:str):
    f = download()
    date = f.name.split('\\')[-1].split('.')[0]
    dtime = datetime.datetime.strptime(date, '%Y%m%d%H%M%S')
    info, data = get_data_section(f, stid)
    fpath = r'D:\酷Q Pro\data\image\skewt\SKEWT_{}.png'.format('_'.join([date, stid]))
    if os.path.exists(fpath):
        return 'skewt/' + 'SKEWT_{}.png'.format('_'.join([date, info[0]]))
    if not info:
        return None
    stationlat = float(info[2])
    data = np.array(data, dtype=float)
    data[data == 9999] = np.nan
    df = pd.DataFrame(data, columns=['pres', 'hght', 'temp', 'dwpt', 'wdir', 'wspd'])
    df_new = df.drop_duplicates('pres', 'last')
    dp_idx = np.where(~np.isnan(df_new.dwpt.values))[0][-1]
    xi = np.arange(0, len(df_new.pres.values), 1)
    p_i = mpi.interpolate_nans_1d(xi, df_new.pres.values) * units.hPa
    t_i = mpi.interpolate_nans_1d(p_i, df_new.temp.values) * units.degC
    td_i = mpi.interpolate_nans_1d(p_i, df_new.dwpt.values) * units.degC
    wind_d_i = mpi.interpolate_nans_1d(p_i, df.wdir.values) * units.degree
    wind_s_i = mpi.interpolate_nans_1d(p_i, df.wspd.values) * units('m/s')
    alt = mpi.interpolate_nans_1d(p_i, df.hght.values) * 10
    u, v = mpcalc.wind_components(wind_s_i, wind_d_i)
    # Resample UV
    index_p100 = get_pressure_level_index(p_i, 100)
    lcl_p, lcl_t = mpcalc.lcl(p_i[0], t_i[0], td_i[0])
    lfc_p, lfc_t = mpcalc.lfc(p_i, t_i, td_i)
    el_p, el_t = mpcalc.el(p_i, t_i, td_i)
    prof = mpcalc.parcel_profile(p_i, t_i[0], td_i[0]).to('degC')
    cape, cin = mpcalc.cape_cin(p_i, t_i, td_i, prof)
    mucape, mucin = mpcalc.most_unstable_cape_cin(p_i, t_i, td_i)
    pwat = mpcalc.precipitable_water(td_i, p_i)
    index_p850 = get_pressure_level_index(p_i, 850)
    index_p700 = get_pressure_level_index(p_i, 700)
    index_p500 = get_pressure_level_index(p_i, 500)
    theta850 = mpcalc.equivalent_potential_temperature(850 * units.hectopascal, t_i[index_p850], td_i[index_p850])
    theta500 = mpcalc.equivalent_potential_temperature(500 * units.hectopascal, t_i[index_p500], td_i[index_p500])
    thetadiff = theta850 - theta500
    k = t_i[index_p850] - t_i[index_p500] + td_i[index_p850] - (t_i[index_p700] - td_i[index_p700])
    a = ((t_i[index_p850] - t_i[index_p500]) - (t_i[index_p850] - td_i[index_p850]) -
         (t_i[index_p700] - td_i[index_p700])-(t_i[index_p500] - td_i[index_p500]))
    sw = c_sweat(np.array(t_i[index_p850].magnitude), np.array(td_i[index_p850].magnitude),
                 np.array(t_i[index_p500].magnitude), np.array(u[index_p850].magnitude),
                 np.array(v[index_p850].magnitude), np.array(u[index_p500].magnitude),
                 np.array(v[index_p500].magnitude))
    si = showalter_index(t_i[index_p850], td_i[index_p850], t_i[index_p500])
    li = lifted_index(t_i[0], td_i[0], p_i[0], t_i[index_p500])
    omega = 7.29e-5
    rdx = 2.87e-3
    kts = (-2 * omega * np.sin(stationlat * np.pi / 180)) / (np.log(500 / 850) * rdx)
    ta = kts * (u[index_p850] * u[index_p500] + v[index_p850] * v[index_p500])
    alpha = 1
    beta = 1
    gamma = 0
    ky = ((beta * ta.magnitude) - si + gamma) / (alpha + (t_i[index_p850].magnitude - td_i[index_p850].magnitude))
    if ta.magnitude < si:
        ky = 0
    fig = plt.figure(figsize=(9, 9), dpi=200)
    skew = SkewT(fig, rotation=30)
    skew.ax.set_ylim(1050, 100)
    skew.ax.set_xlim(-40, 50)
    skew.plot(p_i, t_i, 'r', linewidth=1)
    skew.plot(p_i[:dp_idx+1], td_i[:dp_idx+1], 'g', linewidth=1)
    skew.plot_barbs(p_i[:index_p100], u[:index_p100]*1.94, v[:index_p100]*1.94)
    skew.plot(lcl_p, lcl_t, 'ko', markerfacecolor='black')
    skew.plot(p_i, prof, 'k', linewidth=2)
    if cin.magnitude < 0:
        chi = -1 * cin.magnitude
        skew.shade_cin(p_i, t_i, prof)
    elif cin.magnitude > 0:
        chi = cin.magnitude
        skew.shade_cin(p_i, t_i, prof)
    else:
        chi = 0.
    skew.shade_cape(p_i, t_i, prof)
    skew.plot_dry_adiabats(linewidth=0.5)
    skew.plot_moist_adiabats(linewidth=0.5)
    skew.plot_mixing_lines(linewidth=0.5)
    plt.title('Skew-T Plot @HCl\nStation: {} Time: {}'.format(info[0], dtime.strftime('%Y.%m.%d %H:%M')), fontsize=14, loc='left')

    ax = fig.add_axes([0.95, 0.71, 0.17, 0.17])
    h = Hodograph(ax, component_range=50)
    h.add_grid(increment=20)
    h.plot_colormapped(u[:index_p100], v[:index_p100], alt[:index_p100], linewidth=1.2)
    spacing = -9
    ax.text(-50, -90, 'CAPE: ', fontsize=10)
    ax.text(-50, -90 + spacing, 'CIN: ', fontsize=10)
    ax.text(-50, -90 + spacing * 2, 'MUCAPE: ', fontsize=10)
    ax.text(-50, -90 + spacing * 3, 'PWAT: ', fontsize=10)
    ax.text(-50, -90 + spacing * 4, 'KI: ', fontsize=10)
    ax.text(-50, -90 + spacing * 5, 'AI: ', fontsize=10)
    ax.text(-50, -90 + spacing * 6, 'SWEAT: ', fontsize=10)
    ax.text(-50, -90 + spacing * 7, 'LCL: ', fontsize=10)
    ax.text(-50, -90 + spacing * 8, 'LFC: ', fontsize=10)
    ax.text(-50, -90 + spacing * 9, 'EL: ', fontsize=10)
    ax.text(-50, -90 + spacing * 10, 'SI: ', fontsize=10)
    ax.text(-50, -90 + spacing * 11, 'LI: ', fontsize=10)
    ax.text(-50, -90 + spacing * 12, 'T850-500: ', fontsize=10)
    ax.text(-50, -90 + spacing * 13, 'θse850-500: ', fontsize=10)
    ax.text(-50, -90 + spacing * 14, 'KY: ', fontsize=10)

    ax.text(10, -90, str(np.round_(cape.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing, str(np.round_(chi, 2)), fontsize=10)
    ax.text(10, -90+spacing * 2, str(np.round_(mucape.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing * 3, str(np.round_(pwat.magnitude, 2)), fontsize=10)

    ax.text(10, -90+spacing * 4, str(np.round_(k.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing * 5, str(np.round_(a.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing * 6, str(np.round_(sw, 2)), fontsize=10)
    ax.text(10, -90+spacing * 7, str(np.round_(lcl_p.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing * 8, str(np.round_(lfc_p.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing * 9, str(np.round_(el_p.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing * 10, str(np.round_(si, 2)), fontsize=10)
    ax.text(10, -90+spacing * 11, str(np.round_(li, 2)), fontsize=10)
    ax.text(10, -90+spacing * 12, str(np.round_(t_i[index_p850] - t_i[index_p500], 2).magnitude), fontsize=10)
    ax.text(10, -90+spacing * 13, str(np.round_(thetadiff.magnitude, 2)), fontsize=10)
    ax.text(10, -90+spacing * 14, str(np.round_(ky, 2)), fontsize=10)

    ax.text(45, -90, ' J/kg', fontsize=10)
    ax.text(45, -90+spacing, ' J/kg', fontsize=10)
    ax.text(45, -90+spacing * 2, ' J/kg', fontsize=10)
    ax.text(45, -90+spacing * 3, ' mm', fontsize=10)
    ax.text(45, -90+spacing * 7, ' hPa', fontsize=10)
    ax.text(45, -90+spacing * 8, ' hPa', fontsize=10)
    ax.text(45, -90+spacing * 9, ' hPa', fontsize=10)
    ax.text(45, -90+spacing * 10, ' °C', fontsize=10)
    ax.text(45, -90+spacing * 11, ' °C', fontsize=10)
    ax.text(45, -90+spacing * 12, ' °C', fontsize=10)
    ax.text(45, -90+spacing * 13, ' °C', fontsize=10)

    plt.savefig(fpath, bbox_inches='tight')
    return 'skewt/' + 'SKEWT_{}.png'.format('_'.join([date, info[0]]))

@on_command('SKEWT', only_to_me=False)
async def tk(session:CommandSession):
    ids = get_id(session)
    raw = session.ctx['raw_message'].split('SKEWT')[1].strip()
    command = raw.split(' ')
    if ids not in PERMITUSERS:
        await session.send('此功能为付费功能，请付费后调用')
        raise PermissionError('Permission denied')
    try:
        fp = tlogp(command[0])
    except Exception as e:
        import traceback
        await session.send(traceback.format_exc())
    if fp:
        await session.send('[CQ:image,file={}]'.format(fp))
    else:
        await session.send('无数据')
    plt.close('all')