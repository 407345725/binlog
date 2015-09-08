from __future__ import absolute_import



import redis

from celery.exceptions import Reject
from celery.exceptions import Ignore

from binlog.celery import app
from binlog.tables import *
from binlog.rediskeys import *

from missions.utils import str2time

"""
@summary: 
    http://stackoverflow.com/questions/6835708/retrying-tasks-sent-by-send-task-in-celery
    http://stackoverflow.com/questions/18433071/celery-how-to-send-task-from-remote-machine
    
    
    celery worker --app=project -l info
    celery worker --app=binlog -l info -Q q_task_writerows

"""

redis_conf = {
    'host' : '192.168.1.140',
    'port' : 6379,
    'socket_timeout' : 20000,
}

pool = redis.ConnectionPool(host=redis_conf['host'], port=redis_conf['port'], socket_timeout=redis_conf['socket_timeout'])
redis_client = redis.Redis(connection_pool=pool)
        
@app.task(bind=True, acks_late=True, default_retry_delay=3 * 60)
def update_rows_event(self, table, id, doc):
    # http://docs.celeryproject.org/en/master/userguide/tasks.html
    ignore_flag = False
    if ignore_flag:
        raise Ignore()
    
    try:
        print table,id,doc
        raise Exception
    except Exception as exc:
        # raise self.retry(exc=exc, countdown=60)
        raise Reject(exc, requeue=True)

@app.task(bind=True, acks_late=True, default_retry_delay=3 * 60)
def delete_rows_event(self, table, id):
    try:
        print table,id
        raise Exception
    except Exception as exc:
        #raise self.retry(exc=exc, countdown=60)
        raise Reject(exc, requeue=True)
    
@app.task(bind=True, acks_late=True, default_retry_delay=3 * 60)
def write_rows_event(self, table, id, doc):
    try:
        if table in [APT_DETECTION, IPS_LOG, WEB_DEFEND, DDOS_ATTACK]:
            source_ip = doc.get(u'source_ip', None)
            target_ip = doc.get(u'target_ip', None)
            happened_time = doc.get(u'happened_time')
            
            key_sad, member_sad =  statistics_attacks_day_hour(happened_time)
            key_sak, member_sak = statistics_attacks_type(table)
            
            pipe = redis_client.pipeline()
            pipe.zincrby(key_sak, member_sak, amount=1)
            pipe.zincrby(key_sad, member_sad, amount=1)
            if source_ip:
                key_sas, member_sas = statistics_attacks_sourceip(source_ip)
                pipe.zincrby(key_sas, member_sas, amount=1)
            if target_ip:
                key_sat, member_sat = statistics_attacks_targetip(target_ip)
                pipe.zincrby(key_sat, member_sat, amount=1)
            
            result = pipe.execute()
            print result
    except Exception as exc:
        #raise self.retry(exc=exc, countdown=60)
        print str(exc)
        raise Reject(exc, requeue=True)

