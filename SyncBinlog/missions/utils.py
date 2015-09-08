#!/usr/bin/env python
#encoding=utf-8


import time
import datetime

def str2time(str_time):
    if (isinstance(str_time, str) or isinstance(str_time, unicode)):
        return time.strptime(str_time, '%Y-%m-%d %H:%M:%S')
    else:
        return None
    
if __name__ == "__main__":
    print str2time("2015-08-20 16:55:31")
    datetime.time()
    