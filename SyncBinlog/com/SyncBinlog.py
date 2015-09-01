#!/usr/bin/env python
#encoding=utf-8


# http://www.360doc.com/content/13/1126/10/9934052_332232981.shtml

import json

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['192.168.1.141:9200'])
mysql_settings = {'host': '192.168.1.73',
                  'port': 3306,
                  'user': 'root',
                  'passwd': 'root'}

ignore_table = ['admin', 'admin_role', 'role', 'role_authority']

# real_name=qgswaf_v1
qgswaf_aliases = "qgswaf"

def main():
        stream = BinLogStreamReader(connection_settings = mysql_settings,
                                    server_id = 1,
                                    only_events = [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], 
                                    blocking = True, 
                                    resume_stream = True)
        for binlogevent in stream:
            for row in binlogevent.rows:
                print "%s:%s:" % (binlogevent.schema, binlogevent.table), row
                    
                if binlogevent.table in ignore_table:
                    print "ignore %s"%binlogevent.table, row
                    continue
                    
                if isinstance(binlogevent, DeleteRowsEvent):
                    json_data = json.dumps({
                                        "table" : binlogevent.table,
                                        "action": "delete",
                                        "id": row["values"]["id"]}, default=date_handler) 
                elif isinstance(binlogevent, UpdateRowsEvent):
                    json_data = json.dumps({
                               "table" : binlogevent.table,
                              "action": "update",
                              "id": row["after_values"]["id"],
                              "doc": row["after_values"]}, default=date_handler)
                elif isinstance(binlogevent, WriteRowsEvent):
                    json_data =  json.dumps({
                              "table" : binlogevent.table,
                              "action": "insert",
                              "id": row["values"]["id"],
                              "doc": row["values"]}, default=date_handler)
                    # alieses  = qgswaf_v1
                    res = es.index(index=qgswaf_aliases, doc_type=binlogevent.table, id=row["values"]["id"], body=row["values"])
                    #es.indices.refresh(index="qgswaf")
                print json_data

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


if __name__ == '__main__':
    main()


