from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

result = es.search(index='filebeat*', body={
  'query': {
    'match': {
      'SENDER': 'athagroup.in',
     }
  }
})

#print result['hits']['hits']
for hit in result['hits']['hits']:
    print hit['_score']
    print hit['_source']['SENDER']