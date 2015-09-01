#!/usr/bin/env python
#encoding=utf-8



from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['192.168.1.141:9200'])

es.create(index, doc_type, body, id, params)