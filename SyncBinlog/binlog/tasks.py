from __future__ import absolute_import

from binlog.celery import app
"""

http://stackoverflow.com/questions/6835708/retrying-tasks-sent-by-send-task-in-celery
http://stackoverflow.com/questions/18433071/celery-how-to-send-task-from-remote-machine


celery worker --app=project -l info
celery worker --app=project -l info -Q verify_queue

1 qidong worker
celery worker --app=project -l info -Q add_queue
"""

@app.task
def update_rows_event(table, id, doc):
    print table,id,doc


@app.task
def delete_rows_event(table, id):
    print table,id

@app.task
def write_rows_event(table, id, doc):
    print table,id,doc


