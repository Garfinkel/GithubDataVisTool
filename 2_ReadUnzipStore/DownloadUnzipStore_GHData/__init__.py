import logging
import azure.functions as func
import gzip
import json
import datetime as dt
import time as t
    
    # Blob Trigger - When a file is uploaded to my blob folder (raw). Defined in the functions.json file.
    #   On Trigger -> 
    #       *Read GZIP, Unzip file and turn into JSON
    #       *Filter data to 'new issues' only
    #       *Shape data to meet our requirements
    #       *Store new issues in CosmosDB DBs


def main(inputblob: func.InputStream, outputdb: func.Out[func.Document]):
    logging.info('Data uploaded to raw Blob Storage, function processing a new request.')
    t.sleep(45)
    
    # Store output in variable Data as JSON
    with gzip.open(inputblob) as lines:
        data = [json.loads(i) for i in lines]

    # Create Doc List for batch upload 
    NewEventList = func.DocumentList() 

    # Empty variables
    NER = []
    count = 0
    
    #Log number of records
    records_count = str(len(data))
    logging.info(f'All Records: {records_count}')

    # Format records for CosmosDB Insertion
    for event in data:

        # filter to only issue-related events
        if "Issue" in event["type"]:
    
            # Filter to 'New Issues' only
            if event["payload"]["issue"]["comments"] == 0 :
                
                #Count # of new records in loop
                count += 1

                # Reshape Data + Store Record 
                new_eventrecord = {
                    "id" : event["id"],
                    "issue_id" : event["payload"]["issue"]["id"],
                    "issue_title" : event["payload"]["issue"]["title"],
                    "issue_num" : event["payload"]["issue"]["number"],
                    "repo_id" : event["repo"]["id"],
                    "repo_name" : event["repo"]["name"].rsplit('/',1)[1],
                    "created_datetime" : event["created_at"],
                    "lastupdated_datetime" : event["created_at"],
                    "issue_url" : event["payload"]["issue"]["html_url"],
                    "last_state" : event["payload"]["action"],
                    "NER" : NER,
                    "json" : event
                    }

                #Append Record to list of CosmosDB events for batch
                NewEventList.append(func.Document.from_dict(new_eventrecord))

            # If Issue != New, do nothing
            else:
                pass
                
        
        #if event type != Issue, do nothing
        else:
            pass
            

    t.sleep(5)
    outputdb.set(NewEventList)
    logging.info(f'Total Record Count | New Issue Creation Records: {str(count)}')