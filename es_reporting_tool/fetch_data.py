from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json

def fetch_data()
    '''
        Fetch the data from elasticsearch.
        Apply filter on data before fetching.
        Returns only specified fields
        Convert fetched objects into json format
    '''
    client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    s = Search(using=client, index="filebeat*")
    s = s.query("match", sender="athagroup.in")
    s = s.source(['sender','subject'])
     
    for hit in s.scan():
        print json.dumps(hit, default=lambda o: o.__dict__)