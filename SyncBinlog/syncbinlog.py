#!/usr/bin/env python
#encoding=utf-8

"""
http://docs.jinkan.org/docs/celery/userguide/calling.html
"""

import json

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from datetime import datetime
from binlog.tasks import (
    delete_rows_event,
    update_rows_event,
    write_rows_event
)

mysql_settings = {'host': '192.168.1.73',
                  'port': 3306,
                  'user': 'root',
                  'passwd': 'root'}

ignore_table = ['admin', 'admin_role', 'role', 'role_authority', 'country']

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
                    delete_rows_event.apply_async((binlogevent.table, id), queue="q_task_deleterows", 
                                                  exchange="qgswaf", routing_key="key_deleterows")
                                                  
                elif isinstance(binlogevent, UpdateRowsEvent):
                    json_data = json.dumps({
                               "table" : binlogevent.table,
                              "action": "update",
                              "id": row["after_values"]["id"],
                              "doc": row["after_values"]}, default=date_handler)
                    update_rows_event.apply_async((binlogevent.table, id, row["after_values"]), queue="q_task_updaterows",
                                                  exchange="qgswaf", routing_key="key_updaterows")
                    
                elif isinstance(binlogevent, WriteRowsEvent):
                    json_data =  json.dumps({
                              "table" : binlogevent.table,
                              "action": "insert",
                              "id": row["values"]["id"],
                              "doc": row["values"]}, default=date_handler)
                    write_rows_event.apply_async((binlogevent.table, id, row["values"]), queue="q_task_writerows", 
                                                 exchange="qgswaf", routing_key="key_writerows")
                    
                    
                    # alieses  = qgswaf_v1
                    #res = es.index(index=qgswaf_aliases, doc_type=binlogevent.table, id=row["values"]["id"], body=row["values"])
                    #es.indices.refresh(index="qgswaf")
                print json_data

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

if __name__ == '__main__':
    main()
