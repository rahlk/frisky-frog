import os
import sys
import json
import gzip
import certifi
import logging
import requests
import pandas as pd
from io import BytesIO
from pdb import set_trace
from datetime import datetime
from itertools import product
from urllib3 import PoolManager
from collections import defaultdict
from pathlib import Path, PosixPath
from typing import Dict, Tuple, List, Union, NewType

# Logging Config
logging.basicConfig(format='[+] %(message)s', level=logging.INFO)

# Add project source to path
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))

from crawler import Crawler

# Common types used here.
URL = NewType('URL', str)
DateRange = Tuple[int, int]
Iterable = Union[set, list, tuple]
PandasDataFrame = NewType('PandasDataFrame', pd.core.frame.DataFrame)
Path = NewType('Path', PosixPath)
DataCrawler = NewType('Crawler', Crawler)


class MetricsGetter:
    def __init__(self, top_N_repos: set = None, event_set: set = {
            'PushEvent', 'IssuesEvent', 'PullRequestEvent'}):
        self.data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self.top_N_repos = top_N_repos
        self.event_set = event_set

    def set_top_N_repos(self, repo_set: set) -> None:
        self.top_N_repos = repo_set

    @staticmethod
    def _is_valid_json(possible_json_string: str) -> bool:
        """
        TODO: Move to utils.
        Validates if a provided string is in a valid JSON format

        Parameters
        ----------
        possible_json_str: str
            A possibly invalid JSON string

        Returns
        -------
        bool:
            True is the provided string is a valid json. False otherwise.
        """
        try:
            json.loads(possible_json_string)
        except json.JSONDecodeError:
            return False
        return True

    def _is_event_usable(self, json_data):
        type = json_data["type"]
        if type in self.event_set:
            repo_name = json_data['repo']['name']
            if repo_name in self.top_N_repos:
                return True
        return False

    def _update_data(self, repo, date, event):
        self.data[repo][date][event] += 1

    def _process_pull_request_event(self, data: dict, date: str):
        repo = data['repo']['name']
        payload = data['payload']
        action = payload['action']
        pull_request = payload['pull_request']

        if action == 'opened':
            # New Pull Request has been opened
            self._update_data(repo, date, 'PullRequestOpenRate')

        if action == 'closed':
            # New Pull Request has been opened
            self._update_data(repo, date, 'PullRequestCloseRate')

        if action == 'closed' and pull_request['merged']:
            # New Pull Request has been merged
            self._update_data(repo, date, 'PullRequestMergerRate')

        if action == 'closed' and not pull_request['merged']:
            # New Pull Request has been rejected
            self._update_data(repo, date, 'PullRequestRejectionRate')

    def _process_issue_event(self, data, date):
        repo = data['repo']['name']
        payload = data['payload']
        action = payload['action']
        issue = payload['issue']
        issue_labels = [label['name'] for label in issue['labels']]

        if action == 'opened' or action == 'reopened':
            # New Pull Request has been opened
            self._update_data(repo, date, 'IssueOpenRate')
            # Is it a new bug?
            if 'bug' in issue_labels:
                self._update_data(repo, date, 'BugOpenRate')
            # Is it a new critical bug?
            if 'critical' in issue_labels:
                self._update_data(repo, date, 'CriticalBugOpenRate')
            # Is it a new enhancement?
            if 'enhancement' in issue_labels:
                self._update_data(repo, date, 'EnhancementRequestRate')

        if action == 'closed':
            # An issue has been closed
            # Update AvgIssueCloseTime
            # Multiply Previous issueCloseTime to with IssueResolutionRate
            prevAvgIssueCloseTime = self.data[repo][date]['AvgIssueCloseTime']
            prevIssueResolveRate = self.data[repo][date]['IssueResolutionRate']
            prevIssueCloseTime = prevAvgIssueCloseTime * prevIssueResolveRate
            # Add current issue close time
            issueCreateTime = datetime.strptime(
                issue['created_at'], "%Y-%m-%dT%H:%M:%SZ")
            issueRetireTime = datetime.strptime(
                issue['closed_at'], "%Y-%m-%dT%H:%M:%SZ")
            currentIssueCloseTime = issueRetireTime - issueCreateTime
            # Issue close time in days
            currentIssueCloseTime = currentIssueCloseTime.days
            currentIssueCloseTime += prevIssueCloseTime
            # Increment IssueResolutionRate
            self._update_data(repo, date, 'IssueResolutionRate')
            # Divide current issueClosetime with current IssueResolutionRate\
            currentAvgIssueCloseTime = currentIssueCloseTime / \
                self.data[repo][date]['IssueResolutionRate']
            # Update Average Issue Close Time
            self.data[repo][date]['AvgIssueCloseTime'] = currentAvgIssueCloseTime

            # Has a bug been closed?
            if 'bug' in issue_labels:
                self._update_data(repo, date, 'BugCloseRate')
            # Has a critical bug been closed?
            if 'critical' in issue_labels:
                self._update_data(repo, date, 'CriticalBugCloseRate')

        if action == 'labeled':
            if 'bug' in issue_labels:
                self._update_data(repo, date, 'BugOpenRate')
            else:
                self._update_data(repo, date, 'BugCloseRate')
            if 'critical' in issue_labels:
                self._update_data(repo, date, 'CriticalBugOpenRate')
            else:
                self._update_data(repo, date, 'CriticalBugCloseRate')
            if 'enhancement' in issue_labels:
                self._update_data(repo, date, 'EnhancementRequestRate')

    def _process_commit_event(self, data, date):
        repo = data['repo']['name']
        payload = data['payload']
        num_distinct_commits = payload['distinct_size']
        self.data[repo][date]['CommitRate'] += num_distinct_commits

    def _process_event(self, data, date):
        event_type = data['type']
        if event_type == 'PullRequestEvent':
            self._process_pull_request_event(data, date)
        if event_type == 'IssuesEvent':
            self._process_issue_event(data, date)
        if event_type == 'PushEvent':
            self._process_commit_event(data, date)

    def _url2dictlist(self, mined_url: str) -> None:
        https = PoolManager(cert_reqs='CERT_REQUIRED',
                            ca_certs=certifi.where())
        full_date = mined_url[mined_url.rfind("/") + 1:].split(".")[0]
        date = datetime.strptime(full_date, "%Y-%m-%d-%H").strftime("%m-%d-%Y")
        try:
            response = https.request('GET', mined_url)
            compressed_json = BytesIO(response.data)
            with gzip.GzipFile(fileobj=compressed_json) as json_bytes:
                json_str = json_bytes.read().decode('utf-8')
                for json_value in json_str.split('\n'):
                    if self._is_valid_json(json_value):
                        data = json.loads(json_value)
                        if self._is_event_usable(data):
                            self._process_event(data, date)
        except OSError:
            pass

    def populate(self, crawler: DataCrawler):
        for mined_url in crawler._daterange2url():
            logging.info("Processing {}".format(mined_url))
            all_events = self._url2dictlist(mined_url)

        print(json.dumps(self.data, indent=2),
              file=open('feb_2020_metrics.json', 'w+'))
        set_trace()


if __name__ == "__main__":
    # Initialize data Crawler.
    crawler = Crawler(month=2, year=2020)
    m = MetricsGetter()
    jan_2020 = pd.read_csv(root.joinpath(
        'data', 'monthly', '2020-January.csv'), index_col=0)
    top_100 = set(jan_2020.index.tolist()[:100])
    m.set_top_N_repos(repo_set=top_100)
    m.populate(crawler)
