 #!/usr/bin/env python
# encoding=utf-8

from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['192.168.1.141:9200'])

doc = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}

res = es.index(index="test_v1", doc_type='article', id=1, body=doc)
print(res['created'])

res = es.get(index="test_v1", doc_type='article', id=1)
print(res['_source'])

#  刷新索引(使新加内容对搜索可见)
es.indices.refresh(index="test_v1")

res = es.search(index="test_v1", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
