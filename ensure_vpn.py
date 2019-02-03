from apscheduler.schedulers.blocking import BlockingScheduler
import requests
import datetime

def get_vpn():
    req = requests.get('http://10.1.64.154/', timeout=7)
    print('Task finished at {}'.format(datetime.datetime.now()))
sched = BlockingScheduler()
sched.add_job(get_vpn, 'interval', minutes=30)
sched.start()