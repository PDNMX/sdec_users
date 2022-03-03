import os
from dotenv import load_dotenv
from pymongo import MongoClient
from elasticsearch import Elasticsearch
load_dotenv()

def mongo_to_elastic():
    mongo_user = os.environ.get('MONGO_USER')
    mongo_password = os.environ.get('MONGO_PASSWORD')
    mongo_dbname = os.environ.get('MONGO_DBNAME', 'declaraciones')
    mongo_url = os.environ.get('MONGO_ADDR', 'localhost:27017')

    conn_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_url}/{mongo_dbname}?authSource=admin"
    print (conn_string)

    mongo = MongoClient(conn_string)
    #db = mongo.newmodels
    db = mongo[mongo_dbname]
    # you can use RFC-1738 to specify the url
    es = Elasticsearch([os.environ.get('ES_ADDR', 'https://localhost:9200')])

    # C A R E F U L T H E R E!!
    es.indices.delete(index='users', ignore=[400,404])

    users_mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "filter": {
                    # Fine-tune here!
                    "autocomplete_filter": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 10
                    }
                },
                "analyzer": {
                    "autocomplete": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "autocomplete_filter"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "username": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
                "nombre": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
                "primerApellido": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
                "segundoApellido": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
                "curp": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
                "rfc":{
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                }
            }
        }
    }

    es.indices.create(index='users', body=users_mapping)

    es.indices.refresh(index='users')

    limit = os.environ.get('MONGO_LIMIT', 100)
    limit = int(limit)

    users = db.users.find(
        {}, {'declaraciones': 0, 'refreshJwtToken': 0}
    ).limit(limit)

    print(f'Limit: {limit}')

    for user in users:
        user_doc = {
            'username': user.get('username'),
            'nombre': user.get('nombre'),
            'primerApellido': user.get('primerApellido'),
            'segundoApellido': user.get('segundoApellido'),
            'curp': user.get('curp'),
            'rfc': user.get('rfc'),
            'createdAt': user.get('createdAt'),
            'updatedAt': user.get('updatedAt'),
            'roles': user.get('roles')
        }
        #print (user_doc)
        resp = es.index(index='users', id=user['_id'], body=user_doc)
        print(resp)

if __name__ == '__main__':
    mongo_to_elastic()
