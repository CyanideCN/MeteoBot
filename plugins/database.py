from jsondb.db import Database
import datetime
import time
from os.path import join

# data structure:
'''
{'123456':
    {'EC':
        {'command':
            [('T2M', 120)],
         'count':0,
         'time':1555420298.4835851
        }
    }
}
'''

def _dict(d, k, intype=dict):
    if k not in d.keys():
        d[k] = intype()
    return d[k]

class DBRecord(object):

    def __init__(self):
        self.db = Database(join('database', 'Data_{}.db'.format(datetime.datetime.now().strftime('%Y%m%d'))))

    def write(self, uid, product, command):
        uid = str(uid)
        if uid not in self.db:
            self.db[uid] = dict()
        user = self.db[uid]
        prod = _dict(user, product)
        com = _dict(prod, 'command', list)
        com.append(command)
        cnt = _dict(prod, 'count', int)
        cnt += 1
        _time = _dict(prod, 'time', list)
        _time.append(time.time())
        prod['command'] = com
        prod['count'] = cnt
        prod['time'] = _time
        user[product] = prod
        self.db[uid] = user

    def get_count(self, uid, product):
        uid = str(uid)
        if uid not in self.db:
            return 0
        if product not in self.db[uid].keys():
            return 0
        return self.db[uid][product]['count']

    def weather(self, uid, stid):
        self.write(uid, 'CTX', stid)

    def ec(self, uid, product, fxhour):
        self.write(uid, 'EC', (product, fxhour))

    def gfs(self, uid, product, fxhour):
        self.write(uid, 'GFS', (product, fxhour))

    def skewt(self, uid, stid):
        self.write(uid, 'SKEWT', stid)

    def aws(self, uid, stid):
        self.write(uid, 'AWS', stid)

    def cldas(self, uid, product):
        self.write(uid, 'CLDAS', product)

    def shhr(self, uid, product, fxhour):
        self.write(uid, 'SHHR', (product, fxhour))

    def is_aws_exceed(self, uid):
        return self.get_count(uid, 'AWS') >= 6

    def blacklist(self, uid, group_id, command):
        gid = str(group_id)
        self.write(uid, 'bl_{}'.format(gid), command)

    def get_bl_command(self, uid, group_id):
        uid = str(uid)
        group_id = 'bl_{}'.format(group_id)
        if uid not in self.db:
            return
        user_data = self.db[uid]
        if group_id not in user_data.keys():
            return
        return self.db[uid][group_id]