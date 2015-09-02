from __future__ import absolute_import

from celery import Celery


"""
@see: 
    http://www.lshift.net/blog/2015/04/30/making-celery-play-nice-with-rabbitmq-and-bigwig/
    http://docs.celeryproject.org/en/master/userguide/tasks.html
    pika  requeue = true
    http://codego.net/341934/
"""
app = Celery('binlog',
              broker='amqp://qgs:qwerqwer@192.168.1.123:5672/qgsvhost',
              backend='amqp://',
              include=['binlog.tasks'])


app.conf.update(CELERY_IGNORE_RESULT=True,
                CELERY_ACKS_LATE = True,
                # BROKER_TRANSPORT_OPTIONS = {'confirm_publish': True},
                CELERY_QUEUES = {
                    "default": {
                        "exchange": "default_qgswaf",
                        "exchange_type": "direct",
                        "routing_key": "key_default",
                    },
                    "q_task_updaterows": { 
                        "exchange": "qgswaf",                     
                        "exchange_type": "direct",
                        "routing_key": "key_updaterows",
                    },
                    "q_task_deleterows": { 
                        "exchange": "qgswaf",
                        "exchange_type": "direct",
                        "routing_key": "key_deleterows",
                    },
                    "q_task_writerows": { 
                        "exchange": "qgswaf",
                        "exchange_type": "direct",
                        "routing_key": "key_writerows",
                    },
                },
                CELERY_ROUTES = {
                    'binlog.tasks.update_rows_event': {
                        'queue' : 'q_task_updaterows',
                        'exchange' : 'qgswaf',
                        'routing_key' : 'key_updaterows',
                    },
                    'binlog.tasks.delete_rows_event': {
                        'queue' : 'q_task_deleterows',
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