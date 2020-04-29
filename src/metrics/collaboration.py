import os
import sys
import json
import requests
from tqdm import tqdm
from pdb import set_trace
from time import time, sleep
from datetime import datetime
from datetime import timedelta
from collections import defaultdict
from pathlib import Path

# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

# - Add data dir to path -
data = root.joinpath('data')

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))

GIT_API_URL = "https://api.github.com"

def timed_requests(*args, **kwargs):
    now = time()
    response = requests.get(*args, **kwargs)
    delta = time() - now
    if delta < 1:
        sleep(1 - delta)
    return response

def get_all_comments(repo_name, repo_owner):
    
    list_of_comments = []
    params = {'page': 1, 'per_page': 100}
    there_is_another_page = True
    api_request_url = "{}/repos/{}/{}/issues/comments?since=2020-01-01".format(GIT_API_URL, repo_owner, repo_name)
    while there_is_another_page:
        response = timed_requests(api_request_url, params=params, auth=(
            os.environ['GITHUB_USERNAME'], os.environ['GITHUB_PASSWORD']))

        json_response = json.loads(response.text)
        list_of_comments.extend(json_response)

        if 'next' in response.links:
            api_request_url = response.links['next']['url']
            if params:
                params = {}
        else:
            there_is_another_page = False
    
    return list_of_comments

def get_top_covid_related_repositories():

    covid_url = "https://api.github.com/search/repositories?q=covid+OR+coronavirus+OR+2019-ncov+OR+covid19+OR+SARS-CoV-2+created%3A2020-01-01..2020-04-28&s=forks&per_page=100"
    response = timed_requests(covid_url, auth=(
        os.environ['GITHUB_USERNAME'], os.environ['GITHUB_PASSWORD']))
    json_response = json.loads(response.text)
    top_100_repos = [value['full_name'] for value in json_response['items']]
    return top_100_repos


def build_collaboration_graph():

    assert 'GITHUB_USERNAME' in os.environ, "Please set GITHUB_USERNAME as an environment"
    assert 'GITHUB_PASSWORD' in os.environ, "Please set GITHUB_PASSWORD as an environment"

    top_repos = get_top_covid_related_repositories()
    save_dir = data.joinpath("collaboration", "covid")
    
    for repo in top_repos:
        repo_comment_list = list()
        repo_collab_dict = defaultdict(None)
        repo_name = repo.split('/')[1]
        repo_owner = repo.split('/')[0]
        all_comments = get_all_comments(repo_name, repo_owner)
        size_of_comment_list = len(all_comments)
        for comment in tqdm(all_comments, desc=repo):
            comment_author = comment['user']['login']
            comment_body = comment['body']
            comment_date = comment['updated_at'].split('T')[0]
            parent_issue = comment['issue_url']
            
            response = timed_requests(parent_issue, auth=(
                os.environ['GITHUB_USERNAME'], os.environ['GITHUB_PASSWORD']))
            
            parent_issue_payload = json.loads(response.text)
            parent_issue_author = parent_issue_payload['user']['login']

            repo_collab_dict['id'] = comment['id']
            repo_collab_dict['comment_author'] = comment_author
            repo_collab_dict['body'] = comment_body
            repo_collab_dict['date'] = comment_date
            repo_collab_dict['issue_author'] = parent_issue_author

            repo_comment_list.append(repo_collab_dict)
        
        save_name = date_dir.joinpath('{}-{}.json'.format(repo_owner, repo_name))
        with open(save_name, 'w+') as json_save_path:
            print(json.dumps(repo_comment_list, indent=2), file=json_save_path)
            
    set_trace()

build_collaboration_graph()
