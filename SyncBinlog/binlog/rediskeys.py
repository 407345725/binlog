#!/usr/bin/env python
#encoding=utf-8

import time
import datetime

from binlog.tables import *


"""
@see: 
    (1)、每天攻击次数分布
                 按时间段 statistics.attacks.20150908:1-24
"""

def statistics_attacks_day_hour(current_time):
    '''
    @summary: 返回当前时间攻击次数分布的redis关键字
    @param: current_time 当前时间(datetime类型)
    '''
    #day = time.strftime('%Y%m%d', time.localtime())
    #hour = time.strftime('%H', time.localtime())
    day = current_time.strftime('%Y%m%d')
    hour = current_time.strftime('%H')
    return "statistics.attacks." + day, hour

def statistics_attacks_sourceip(sourceip):
    '''
    @summary: 源ip地址统计攻击总次数
    @param:  sourceip 源IP地址
    '''
    return "statistics.attacks.sourceip", str(sourceip)


def statistics_attacks_targetip(targetip):
    '''
    @summary: 被攻击目标攻击总次数
    @param:  sourceip 源IP地址
    '''
    return "statistics.attacks.targetip", str(targetip)

def statistics_attacks_type(table):
    '''
    @summary: 统计(ddos, webd, ips, apt)4种类型攻击总次数
    @param:  kind 类别
    '''
    #table in [APT_DETECTION, IPS_LOG, WEB_DEFEND, DDOS_ATTACK]
    kind = 'ddos'
    if table == APT_DETECTION:
        kind = 'apt'
    elif table == IPS_LOG:
        kind = 'ips'
    elif table == WEB_DEFEND:
        kind = 'web'    
    elif table == DDOS_ATTACK:
        kind = 'ddos'
    return "statistics.attacks.type", str(kind)

if __name__ == "__main__":
    print statistics_attacks_day_hour(datetime.datetime.now())
    print statistics_attacks_sourceip("192.168.1.1")
    



