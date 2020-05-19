from azure.cosmos import CosmosClient, PartitionKey, errors
import azure.cosmos.cosmos_client as cosmos_client
import json
import pandas as pd
import datetime as dt
from collections import Counter

### Establish connection string with DB  ###    
import os

def connectionstring():

    HOST = os.environ["AccountEndpoint"]
    MasterKey = os.environ["AccountKey"]
    CosmosDB_db = os.environ["Database"]
    CosmosDB_container = os.environ["Container"]

    # Initialize the Cosmos client
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MasterKey}) 

    #Grab DB + Container client
    db_client = client.get_database_client(CosmosDB_db)
    container = db_client.get_container_client(container=CosmosDB_container)

    return container


### Query DB for all items in a given repo by Id  ###    
def DBQuery_AllIssueByRepoId(repo_id):

    container = connectionstring()

    #Query container
    allitems = container.query_items(
    query="SELECT * FROM table2 f WHERE f.NER <> [] AND f.repo_id = @id",
    parameters=[{ "name":"@id", "value": repo_id}],
    enable_cross_partition_query=True)
    
    return allitems

### Query DB for all items in a given repo by name  ###    
def DBQuery_AllIssueByRepoName(repo_name):

    container = connectionstring()

    #Query container
    allitems = container.query_items(
    query="SELECT * FROM table2 f WHERE f.NER <> [] AND f.repo_name = @id",
    parameters=[{ "name":"@id", "value": repo_name}],
    enable_cross_partition_query=True)
    
    return allitems


### Convert query results into DF ### 
def DBToDF(itemsFromQuery): 
 
    result_list = []
    for item_DBToDF in itemsFromQuery:
        
        # Reformat datetime to startofweek
        try:
            issuedate = dt.datetime.strptime(item_DBToDF["created_datetime"],"%Y-%m-%dT%H:%M:%SZ")
            startofweek = dt.datetime.strftime(issuedate - dt.timedelta(days=issuedate.weekday()),"%Y-%m-%d")
            item_DBToDF["created_datetime"] = str(startofweek)
        except: pass
        

            # Append to empty list
        result_list.append(item_DBToDF)

    # Drop results into dataframe
    df = pd.DataFrame.from_records(result_list)
    #df = df.drop_duplicates()
    return df


### Count issues by week from DF ###    
def RepoWeeklyStats(df_input):

    # Count issues per week #
    issues_per_week = df_input.groupby('created_datetime').count()

    return issues_per_week


### Count issues by key phrase from DF ###    
def RepoTopPhrases(df_input):

       
    # Create list of all key issues 
    key_phrases = []
    for x,y in df_input.iterrows():
        for NER_entities in y['NER']:
            key_phrases.append(NER_entities)

    # count occurance and turn into dict
    c = dict(Counter(key_phrases))

    # turn count into df
    df_final = pd.DataFrame(list(c.items()), columns=['NER', 'Count']).sort_values("Count",ascending=False)

    return df_final


### Query for a single item ###
def DBSingleIssueQuery(IssueItemId):
    container = connectionstring()

    #Query container
    SI_items = container.query_items(
    query="SELECT Top 1 * FROM FinalDB i WHERE i.id=@id",
    parameters=[{ "name":"@id", "value": IssueItemId}],
    enable_cross_partition_query=True)

    # Loop through results, and assign variables
    for SI_record in SI_items:
        base = SI_record
        
    return base
    
def DBSingleIssueLookup(IssueItemId, repoName):
        container = connectionstring()
        item_response = container.read_item(item=IssueItemId, partition_key=repoName)

        return item_response


##################
## HOW TO QUERY ##

# 1) Query for single item
#SingleIssueResult = DBSingleIssueQuery('10459kk064042')

# 2) Query for Repo aggregate data

# Return DF
#BaseItems = DBQuery_AllIssueByRepo(73786682)
#df = DBToDF(BaseItems)
#TopIssues = RepoTopPhrases(df)

# Run Calcs
#WeeklyIssueCount = RepoWeeklyStats(df)
#TopIssues = RepoTopPhrases(df,500)



