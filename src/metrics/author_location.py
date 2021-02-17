import os
import sys
import json
import requests
from time import time, sleep
from pathlib import Path
from collections import defaultdict
from contextlib import ContextDecorator

# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))


if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))


class AuthorLocation(ContextDecorator):
    
    def __init__(self):
        pass


    def __enter__(self):
        # - Add data dir -
        self.data_path = root.joinpath('data')
        # - Add save dir -
        self.save_dir = self.data_path.joinpath("collaboration", "covid", "top-9-repos", "locations")
        # - Container to hold saved results -
        self.all_repos = defaultdict(None)
        # - API URL -
        self.GIT_API_URL = "https://api.github.com"
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

    def get_locations(self, data):
        """
        Return a dict of all locations of users by day in a repo

        Parameters
        ----------
        data: dict
            adjacency matrix of interactions by day

        Returns
        -------
        dict[Dict]:
            A dict with keys by date of all locations of users
        """
        location_matrices = {}
  
        for date, users in data.items():
            location_matrices[date] = {}
            for user in users.keys():
                api_request_url = "{}/users/{}".format(self.GIT_API_URL, user)

                response = self.timed_requests(api_request_url, auth=(
                    os.environ['GITHUB_USERNAME'], os.environ['GITHUB_PASSWORD']))
                
                json_response = json.loads(response.text)

                location = json_response["location"]
                
                #check location is not null
                if location:
                    print(location)
                    if location in location_matrices[date].keys():
                        location_matrices[date][location] += 1
                    else:
                        location_matrices[date][location] = 1
                        
        return location_matrices

    def __call__(self):
        #iterate through all json files in collaboration/covid directory
        os.chdir('/local/jyh2131/frisky-frog/data/collaboration/covid/top-9-repos/test123')

        fileList = []

        for root, dirs, files in os.walk('/local/jyh2131/frisky-frog/data/collaboration/covid/top-9-repos/test123'):
            
            for name in files:
                fileList.append(os.path.join(root, name))
            
        for file in fileList:
            
            with open(file, 'r') as currentFile:
                data = json.load(currentFile)

                #returns a dict with keys as dates and values as array of comment ids
                location_matrices = self.get_locations(data)
        
                #put repo into all_repos dict
                repo_name = file.rsplit('/', 1)[1]
                
                self.all_repos[repo_name] = location_matrices


    def __exit__(self, exc, value, traceback):
        for repo, matrices_dict in self.all_repos.items():
            
            save_name = self.save_dir.joinpath('{}-{}'.format('locations', repo))
            with open(save_name, 'w+') as json_save_path:
                print(json.dumps(matrices_dict, indent=2), file=json_save_path)      


if __name__ == "__main__":
    with AuthorLocation() as author_location:
        author_location()