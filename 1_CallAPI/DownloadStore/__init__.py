import azure.functions as func
from azure.storage.blob import BlobClient
import os
import logging
import gzip
import requests

def main(req: func.HttpRequest):
    logging.info('Github Function triggered function processed a request.')

    # URL Input Parameters
    input_year = int(req.params.get('year'))
    input_month = int(req.params.get('month'))
    input_day = int(req.params.get('day'))
    input_hour = int(req.params.get('hour'))

    # Example : http://localhost:7071/api/DownloadStore?year=2020&month=05&day=11&hour=14


    # Call API passing in date parameters  to obtain zip file
    domain = 'http://data.gharchive.org/'
    path = "{y}-{m:02d}-{d:02d}-{h}.json.gz"
    p = path.format(y=input_year, m=input_month, d=input_day, h=input_hour)
    uri = domain + p
    logging.info(f'Getting file for {uri}')
    r = requests.get(uri, stream=True)

    #Log Header Details
    logging.info(r.headers['Content-Length'])

    # Call Azure Python SDK, Upload API response
    logging.info(f'Uploading Starting - {p}')
    connection_string = os.environ["CONNECTION_STRING"]

    try:
        blob = BlobClient.from_connection_string(conn_str=connection_string, container_name='raw', blob_name=p)
        blob.upload_blob(r.content, overwrite = True) 
        logging.info(f'Uploading Complete - {p}')    
    except:
        logging.error(f'Upload Failure - {p}')    
    
    return 'Function has finished running'

    # LOCAL : http://localhost:7071/api/DownloadStore?year=2020&month=05&day=11&hour=14
    # PARAMS : &year=2020&month=05&day=09&hour=17