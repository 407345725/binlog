from __future__ import absolute_import

from celery import Celery

app = Celery('project',
              broker='amqp://qgs:qwerqwer@192.168.1.123:5672/qgsvhost',
              backend='amqp://',
              include=['binlog.tasks'])

app.conf.update(CELERY_IGNORE_RESULT=True,
                CELERY_ROUTES = {
                    'binlog.tasks.update_rows_event': {
                        'queue' : 'q_task_updaterows',
                        'exchange' : 'qgswaf',
                        'routing_key' : 'key_updaterows',
                    },
                    'binlog.tasks.delete_rows_event': {
                        'queue' : 'key_deleterows',
                        'exchange' : 'qgswaf',
                        'routing_key' : 'key_deleterows',
                    },
                    'binlog.tasks.write_rows_event': {
                        'queue' : 'q_task_writerows',
                        'exchange' : 'qgswaf',
                        'routing_key' : 'key_writerows',
                    },
                                  
                },)


if __name__ == '__main__':
    app.start()