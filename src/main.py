# Import all required library
from sodapy import Socrata
import sys 
import requests 
import argparse
import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth
from os import environ
import time

#Add arguement to python script
parser = argparse.ArgumentParser()
parser.add_argument('--page_size', type=int, help="Input the number of how many records to request from the API per page.",required = True)
parser.add_argument('--num_pages', type=int, help = 'Input the number of how many pages to get in total.')
args = parser.parse_args()

# All required parameter
num_pages = int(args.num_pages)
page_size = int(args.page_size)
app_token = environ['APP_TOKEN']
dataset_id= environ['DATASET_ID']
es_username =environ['ES_USERNAME']
es_password = environ['ES_PASSWORD']
es_host = environ['ES_HOST']

# Function to connect with Elasticsearch and creaet es object
def es_connection(es_username,es_password,es_host):
    es = Elasticsearch(
        hosts=[es_host],
        http_auth=HTTPBasicAuth(
        es_username,
        es_password),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
    return es

# Mapping the data to ES object format and reset index
def create_index(es):
    settings= {
        "settings":{
            "number_of_shards":1,
            "number_of_replicas":1
        },
         "mappings": {
            "properties": {    
                "plate":{"type": "text"},
                "state":{"type":"text"},
                "liscense_type":{"type":"text"},
                "summons_number":{"type":"float"},
                "issue_date":{"type":"date"},
                "violation_time":{"type":"text"},
                "violation":{"type":"text"},
                "judgment_entry_date":{"type":"text"},
                "fine_amount":{"type":"float"},
                "penalty_amount":{"type":"float"},
                "interest_amount":{"type":"float"},
                "reduction_amount":{"type":"float"},
                "payment_amount":{"type":"float"},
                "amount_due":{"type":"float"},
                "precinct":{"type":"text"},
                "county":{"type":"text"},
                "issuing_agency":{"type":"text"},
                "violation_status":{"type":"text"},
                "summons_image":{"type":"URL"}
                }
            }        
        }
    
    es.indices.create(index='parking', ignore=400, body=settings)
    print('Created Index')
    
    return

def data_trans(record):
    record['fine_amount'] = float(record['fine_amount'])
    record['penalty_amount'] = float(record['penalty_amount'])
    record['interest_amount'] = float(record['interest_amount'])
    record['reduction_amount'] = float(record['reduction_amount'])
    record['payment_amount'] = float(record['payment_amount'])
    record['amount_due'] = float(record['amount_due'])
    record['issue_date'] = datetime.datetime.strptime(record['issue_date'], '%m/%d/%Y')
    
    return record

#Upload the data to elastic search 
def upload_to_es():
    result = client.get(dataset_id, limit=page_size, offset= number_sum)
    count=0
    for record in result:
        try:
            es.index(index='parking',body=data_trans(record))
            count +=1 
            
        except (RuntimeError, TypeError, NameError, KeyError, ValueError):
            continue
    return count

#Run the process
if __name__ == '__main__':
    start=datetime.datetime.now()
    client = Socrata("data.cityofnewyork.us", app_token)
    es = es_connection(es_username,es_password,es_host)
    create_index(es)
    if num_pages is not None:
        number_sum = 0
        for i in range(int(num_pages)):
            rowtime = datetime.datetime.now()
            
            count = upload_to_es()
            number_sum += count 
            print('Page',i+1,'\t Succes:',count,'\tSkipped:',page_size - count, '\tRun time:', datetime.datetime.now()-rowtime)
            
            
        print('_________________________________________________________________________________________________________')        
        print('Total records loaded:', number_sum,'\nTotal pages:',num_pages,'\nRecords on each page:',page_size,'\nTotal invalid data:',num_pages * page_size - number_sum)    
    else:
        total = client.get(dataset_id, select='COUNT(*)')[0]['COUNT']
        number_sum = 0
        page = total / page_size
        for i in range(int(page)):
            
            count = upload_to_es()
            number_sum += count
            print('Page',i+1,'\t Succes:',count,'\tSkipped:',page_size - count, '\tRun time:', datetime.datetime.now()-rowtime)
            

            
        print('_________________________________________________________________________________________________________')         
        print('Page',i+1,'\t Succes:',count,'\tSkipped:',page_size - count, '\tRun time:', datetime.datetime.now()-rowtime)          
        print('All valid data of this dataset has been loaded to ElasticSearch.')

    print('Total run time: ',datetime.datetime.now()-start) 
    print('_________________________________________________________________________________________________________')
    
   
    
   