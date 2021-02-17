import os
import sys
import json
import requests
import pandas as pd
import numpy
from tqdm import tqdm
from pathlib import Path
from pdb import set_trace
from time import time, sleep
from datetime import datetime
from datetime import timedelta
from collections import defaultdict
from contextlib import ContextDecorator


# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))


if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))


class NonCovidCollaborationGraph(ContextDecorator):
    
    def __init__(self):
        pass

    def __enter__(self):
        # - Add data dir -
        self.data_path = root.joinpath('data')
        # - Add save dir -
        self.save_dir = self.data_path.joinpath("collaboration", "non-covid", "top-10-repos", "comments")
        # - API URL -
        self.GIT_API_URL = "https://api.github.com"
        # - Container to hold saved results -
        self.all_repos = defaultdict(None)
        return self
    
    @staticmethod
    def timed_requests(*args, **kwargs):
        """
        Throttles download limit to prevent exceeding github's rate limit of 5000 requests/sec.
        """
        now = time()
        response = requests.get(*args, **kwargs)
        delta = time() - now
        if delta < 0.75:
            sleep(0.75 - delta)
        return response

    def get_all_comments(self, repo_name, repo_owner, since="2020-01-31"):
        """
        Return a list of all comments made on a repo.

        Parameters
        ----------
        repo_name: str
            Name of repo.
        repo_owner: str
            Owner of the repo.
        since: str
            Date to begin mining from.

        Returns
        -------
        list[Dict]:
            A list containing all the response json as a dictionary.
        """
        list_of_comments = []
        params = {'page': 1, 'per_page': 100}
        there_is_another_page = True
        api_request_url = "{}/repos/{}/{}/issues/comments?since={}".format(self.GIT_API_URL, repo_owner, repo_name, since)
        while there_is_another_page:
            response = self.timed_requests(api_request_url, params=params, auth=(
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


    def get_top__non_covid_related_repositories(self):
        """
        Returns top 10 non covid related repos from /local/rkrsn/frisky-frog/data/active_repos.csv .
        TODO: Parameterize the number (currently 10)

        Returns
        -------
        list[str]:
            A list of repos each item in the list is a string formated as :owner/:name
        """
        #get top 10 repos from csv
        active_repos = pd.read_csv("/local/rkrsn/frisky-frog/data/active_repos.csv")
        df = active_repos.Repository.tail(48) 
        top_10_repos = df.values.tolist()
        return top_10_repos[0:4]


    def __call__(self):
        top_repos = self.get_top__non_covid_related_repositories()
        for repo in top_repos:
            repo_collab_dict = defaultdict(None)
            repo_name = repo.split('/')[1]
            repo_owner = repo.split('/')[0]
            all_comments = self.get_all_comments(repo_name, repo_owner)
            
            for comment in tqdm(all_comments, desc=repo):
                comment_id = comment["id"]
                comment_author = comment['user']['login']
                comment_body = comment['body']
                comment_date = comment['updated_at'].split('T')[0]
                parent_issue = comment['issue_url']
                
                response = self.timed_requests(parent_issue, auth=(
                    os.environ['GITHUB_USERNAME'], os.environ['GITHUB_PASSWORD']))
                
                parent_issue_payload = json.loads(response.text)
                try:
                    parent_issue_author = parent_issue_payload['user']['login']
                except KeyError:
                    parent_issue_author = None

                repo_collab_dict[comment_id] = defaultdict(None)

                repo_collab_dict[comment_id]['comment_author'] = comment_author
                repo_collab_dict[comment_id]['body'] = comment_body
                repo_collab_dict[comment_id]['date'] = comment_date
                repo_collab_dict[comment_id]['issue_author'] = parent_issue_author

            save_name = self.save_dir.joinpath('{}-{}.json'.format(repo_owner, repo_name))
            with open(save_name, 'w+') as json_save_path:
                print(json.dumps(repo_collab_dict, indent=2), file=json_save_path)
            
            self.all_repos[repo] = repo_collab_dict

    def __exit__(self, exc, value, traceback):
        for repo, comments_dict in self.all_repos.items():
            repo_name = repo.split('/')[1]
            repo_owner = repo.split('/')[0]
            save_name = self.save_dir.joinpath('{}-{}.json'.format(repo_owner, repo_name))
            with open(save_name, 'w+') as json_save_path:
                print(json.dumps(comments_dict, indent=2), file=json_save_path)



if __name__ == "__main__":
    assert 'GITHUB_USERNAME' in os.environ, "Please set GITHUB_USERNAME as an environment"
    assert 'GITHUB_PASSWORD' in os.environ, "Please set GITHUB_PASSWORD as an environment"

    with NonCovidCollaborationGraph() as collab_graph:
        collab_graph()


    
