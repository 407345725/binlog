#!/usr/bin/env python
#encoding=utf-8


# http://www.360doc.com/content/13/1126/10/9934052_332232981.shtml

import json
import cherrypy

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)



mysql_settings = {'host': '192.168.1.73',
                  'port': 3306,
                  'user': 'root',
                  'passwd': 'root'}


ignore_table = ['admin', 'admin_role', 'role', 'role_authority']

class Streamer(object):
    def __init__(self):
        self.stream = BinLogStreamReader(connection_settings = mysql_settings,
                                         server_id = 1,
                                         only_events = [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], 
                                         blocking = True, 
                                         resume_stream = True)


    def index(self):
        cherrypy.response.headers['Content-Type'] = 'text/plain'
        def content():
            for binlogevent in self.stream:
                for row in binlogevent.rows:
                    print "%s:%s:" % (binlogevent.schema, binlogevent.table), row
                    
                    if binlogevent.table in ignore_table:
                        continue
                    
                    if isinstance(binlogevent, DeleteRowsEvent):
                        yield json.dumps({
                              "action": "delete",
                              "id": row["values"]["id"]}, default=date_handler) + "\n"
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        yield json.dumps({
                              "action": "update",
                              "id": row["after_values"]["id"],
                              "doc": row["after_values"]}, default=date_handler) + "\n"
                    elif isinstance(binlogevent, WriteRowsEvent):
                        yield json.dumps({
                              "action": "insert",
                              "id": row["values"]["id"],
                              "doc": row["values"]}, default=date_handler) + "\n"

        return content()

    index.exposed = True
    index._cp_config = {"response.stream": True}


def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

conf = {'global': {'server.socket_port': 3721, 'server.socket_host': '127.0.0.1'}} 
cherrypy.config.update(conf)
   
cherrypy.quickstart(Streamer())

