from flask import Flask, render_template, request, redirect, url_for
from cosmosfunc.cosmos import DBSingleIssueQuery, DBQuery_AllIssueByRepoName, DBToDF, RepoWeeklyStats, RepoTopPhrases, DBSingleIssueLookup
import pandas as pd

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.htm')

@app.route('/repo')
def repo():
    return render_template('repo.htm')

@app.route('/r/<name>')
def repo_details(name):

    # Pulls all issues for a given repo. 
    # Helper method from 'cosmosfunc' folder
    BaseItems = DBQuery_AllIssueByRepoName(name)
    df = DBToDF(BaseItems)
    
    # Calculate for overview
    TotalIssues = df['id'].count()
    RepoId = df['repo_id'][0]
    RepoName = df['repo_name'][0]
    MostRecent = df['lastupdated_datetime'].max() 
    overview_data = {
        'RepoName' : RepoName,
        'RepoId' : RepoId,
        'TotalIssues' :  TotalIssues,
        'MostRecent' : MostRecent
    }

    # Calculate top key phrases
    df_TopIssues = RepoTopPhrases(df)
    df_TopIssues['Pct'] = round(df_TopIssues['Count'] / TotalIssues,2)
    df_TopIssues = df_TopIssues.head(10)
    df_TopIssues['RepoName'] = RepoName


    # Calculate most recent
    columns = ['id','issue_title','created_datetime','issue_url','NER','repo_name']
    df_mostrecent = df[columns]
    df_mostrecent = df_mostrecent.sort_values('created_datetime',ascending = 0).head(10)

    return render_template('repoSummary.htm',overview_data = overview_data, recentIssue=df_mostrecent, TopIssues=df_TopIssues)

@app.route('/repoSummary',methods = ['POST', 'GET'])
def repoSummary():
   if request.method == 'POST':
      result = request.form
      return redirect(f"r/{result['RepoName']}", code=301)

@app.route('/issue')
def issue():
    return render_template('issue.htm')

@app.route('/issueSummary',methods = ['POST', 'GET'])
def issuesummary():
   if request.method == 'POST':
      result = request.form
      return redirect(f"r/{result['RepoName']}/i/{result['IssueId']}", code=301)

@app.route('/r/<reponame>/i/<issueid>')
def issuedeepdive(reponame, issueid):
    results = DBSingleIssueLookup(IssueItemId=issueid,repoName=reponame)
    return render_template('issueSummary.htm',singleitem = results)

@app.route('/r/<reponame>/kp/<keyphrase>')
def keyphrasedeepdive(reponame, keyphrase):
    
    # Query DB for Repo Items
    BaseItems = DBQuery_AllIssueByRepoName(reponame)
    df = DBToDF(BaseItems)
    
    # Trim DF
    columns = ['id','issue_title','created_datetime','issue_url','NER','repo_name']
    df_keyphrase = df[columns]
    
    # Filter to KeyPhrase
    filter = keyphrase
    df_keyphrase = df_keyphrase[df_keyphrase['NER'].str.contains(filter, regex=False)]
    df_keyphrase = df_keyphrase.sort_values('created_datetime',ascending = 0)
    return render_template('keyphraseSummary.htm',output = df_keyphrase)
    
if __name__ == '__main__':
    app.run()

