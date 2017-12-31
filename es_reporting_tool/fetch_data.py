from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json
import os.path

def file_cleanup():
    '''
    Remove old files and
    '''
    try:
        os.remove('data.json')
    except OSError:
        pass
        
    try:
        os.remove('data.csv')
    except OSError:
        pass

def fetch_data():
    '''
    Fetch the data from elasticsearch.
    Apply filter on data before fetching.
    Returns only specified fields
    Convert fetched objects into json format and write into the file.
    '''
    client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    s = Search(using=client, index='filebeat*')
    s = s.query('match', sender='athagroup.in')
    s = s.source(['sender','subject'])
    
    fh = open('data.json', 'w')
     
    for hit in s.scan():
        fh.write(json.dumps(hit, default=lambda o: o.__dict__))

    fh.close()
        
if __name__ == "__main__":
    file_cleanup()
    #fetch_data()