import logging
import os, requests, uuid, json
import azure.functions as func
from azure.cognitiveservices.language.textanalytics import TextAnalyticsClient
from msrest.authentication import CognitiveServicesCredentials

def main(documents: func.DocumentList, outdoc: func.Out[func.Document]):
    if documents:
        logging.info('Total Documents: %s', str(len(documents)))


    # Grab Env Variables 
    #   Local Development = local.settings.json
    #   In Production = application settings in Azure
    Cognitive_Endpoint = os.environ['Cognitive_Endpoint']
    Cognitive_Key = os.environ['Cognitive_Key']
        
    # SDK Auth Flow
    credentials = CognitiveServicesCredentials(Cognitive_Key)
    text_analytics = TextAnalyticsClient(endpoint=Cognitive_Endpoint, credentials=credentials)

    # Create Doc List to append each doc to
    eventlist = func.DocumentList() 

    # Set Batch Variables
    batch_size = 100
    batch_job = []
    computed_batches = []


    # look through every issue, filter to issue creation and set variables
        #check NER to avoid recursive loop
    for documents_items in documents:
        if len(documents_items["NER"]) == 0:
            eventId = documents_items["id"]
            IssueTitle = documents_items["issue_title"]

            #Create dict to pass to cognitive service issue titles
            doc_phrase_dict = {
                "id": eventId,
                "language": "en",
                "text": IssueTitle
                }
                    
            # Append to batch_job for batch call
            batch_job.append(doc_phrase_dict)
            
            # Call API every 100 docs or we reach the end of the document list
            if len(batch_job)>=batch_size or eventId == documents[-1]["id"]:
            
                # Pass Batch to SDK
                response = text_analytics.key_phrases(documents=batch_job)
                
                # Loop through each item and update the original document
                for response_items in response.documents: 
                                                   
                    # Apply NER findings back to each document
                    computed_batches.append(response_items)

    
    # Now we loop through the original list and join the NER cells back
    for documents_items in documents:
        if len(documents_items["NER"]) == 0:
            for x in computed_batches:
                if x.id == documents_items["id"]:
                    
                    # Update NER and append to list
                    documents_items["NER"] = x.key_phrases
                    eventlist.append(func.Document.from_dict(documents_items))

                    # Delete item from computed batches
                    # HELP! Can't figure out!
                    # want to do something like: del computed_batches[x.id]



    ## Set the DocumentList to outdoc to store into CosmosDB using CosmosDB output binding
    logging.info("Item Count: %s" % (len(eventlist)))
    outdoc.set(eventlist)




