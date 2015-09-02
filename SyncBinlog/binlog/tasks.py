from __future__ import absolute_import



from celery.exceptions import Reject
from celery.exceptions import Ignore


from binlog.celery import app
"""

http://stackoverflow.com/questions/6835708/retrying-tasks-sent-by-send-task-in-celery
http://stackoverflow.com/questions/18433071/celery-how-to-send-task-from-remote-machine


celery worker --app=project -l info
celery worker --app=project -l info -Q verify_queue

1 qidong worker
celery worker --app=project -l info -Q add_queue
"""

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
def write_rows_event(self,  table, id, doc):
    try:
        print table,id,doc
        raise Exception
    except Exception as exc:
        #raise self.retry(exc=exc, countdown=60)
        raise Reject(exc, requeue=True)

