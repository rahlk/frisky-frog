import os
import sys
import json
import gzip
import certifi
import logging
import requests
import pandas as pd
from io import BytesIO
from ipdb import set_trace
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

# Common types used here.
URL = NewType('URL', str)
DateRange = Tuple[int, int]
Iterable = Union[set, list, tuple]
PandasDataFrame = NewType('PandasDataFrame', pd.core.frame.DataFrame)
Path = NewType('Path', PosixPath)


class Crawler:
    def __init__(self, hour: Union[DateRange, int] = (0, 23),
                 date: Union[DateRange, int] = (1, 31),
                 month: Union[DateRange, int] = (1, 12),
                 year: Union[DateRange, int] = (2019, 2020),
                 event_set: set = {'PushEvent', 'ForkEvent', 'StarEvent',
                                   'LabelEvent', 'IssuesEvent', 'StatusEvent',
                                   'PullRequestEvent', 'IssuesCommentEvent',
                                   'CommitCommentEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent'}):
        """
        A crawler for GH Archive (https://www.gharchive.org/).

        Query GH Archive and covert data in to a format that can be processed with pandas, csv, etc.

        Parameters
        ----------
        hour: Tuple(Int, Int) (default=(0, 23))
            Hourly data as a tuple of start and end values.
        date: Tuple(Int, Int) (default=(1, 31))
            Daily data as a tuple of start and end values.
        month: Tuple(Int, Int) (default=(1, 12))
            Monthly data as a tuple of start and end values.
        year: Tuple(Int, Int) (default=(2019, 2020))
            Yearly data as a tuple of start and end values.
        event_set: set
            A set of events to filter repositories by.

        Notes
        -----
        + For specific values use Discrete numbers.
        E.g., for April, 20th 2019, 4:00 PM provide: hour=16, date=20, month=4, year=2019.

        + For ranges provide a tuple of start and end values
        E.g., for first half of 2019 provide: hour=(0, 23), date=(1, 31), month=(1, 6), year=2019.

        + Events of interest:
            - Push
            - Forks
            - Stars
            - Label
            - Issues
            - Status
            - Issue Comment
            - Commit Comment
            - Pull Request Event
        """
        self.date = date
        self.year = year
        self.hour = hour
        self.month = month
        self.event_set = event_set
        self.mined_data_df = defaultdict(lambda: None)

    def set_date_range(self, hour: Union[DateRange, int] = (0, 23),
                       date: Union[DateRange, int] = (1, 31),
                       month: Union[DateRange, int] = (1, 12),
                       year: Union[DateRange, int] = (2019, 2020)):
        logging.info(" CRAWLER: Updating date range")
        self.date = date
        self.year = year
        self.hour = hour
        self.month = month

    def update_eventset(self, new_eventset: Iterable) -> None:
        """
        A setter method used to update the eventset.

        Parameters
        ----------
        new_eventset: Iterable (i.e, set, list, tuple)

        """
        assert isinstance(new_eventset, (set, list, tuple)
                          ), "The parameter new_eventset is not any of 'set', 'list', 'tuple'."
        self.event_set = set(new_eventset)

    @staticmethod
    def _inclusive_range(start: int, end: int) -> List:
        """
        An inclusive range method.

        Parameters
        ----------
        start: int
            Starting index
        end: int
            Ending index

        Returns
        -------
        list:
            A list of all the numbers in the range (including the end).

        Examples
        --------
        + _inclusive_range(1,5) :-> [1, 2, 3, 4, 5]
        + _inclusive_range(2,2) :-> [2]

        """
        return list(range(start, end + 1))

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

    def _daterange2url(self) -> URL:
        """
        TODO: Move to Utils
        Converts user provided date range into a GH Archive URL.

        GH Archive uses a specific string format to encode the data url. This method formats user provided date-range into a URL that can be queried.

        Yields
        ------
        str:
            Download URL for the GH Archive data
        """

        # If no range is provided, convert to a 2 element tuple. It's just
        # much easier to generate a string this way.
        if not isinstance(self.hour, tuple):
            self.hour = self.hour, self.hour

        if not isinstance(self.date, tuple):
            self.date = self.date, self.date

        if not isinstance(self.month, tuple):
            self.month = self.month, self.month

        if not isinstance(self.year, tuple):
            self.year = self.year, self.year

        hour = self._inclusive_range(*self.hour)
        year = self._inclusive_range(*self.year)
        date = self._inclusive_range(*self.date)
        month = self._inclusive_range(*self.month)

        # Generate a cartesian product of all the date-time combinations
        all_timestamps = tuple(product(year, month, date, hour))

        for yy, mm, dd, hh in all_timestamps:
            yield 'https://data.gharchive.org/{:02d}-{:02d}-{:02d}-{}.json.gz'.format(yy, mm, dd, hh)

    @staticmethod
    def filter_by_event(json_data: dict, filter_set: set) -> bool:
        """
        Filters an event based on a user provided set of events.

        Parameters
        ----------
        json_data: dict
            The JSON payload to verify.
        filter_set: set
            The set of filters to match against.

        Returns
        -------
        bool:
            True if the current event is in the selected events
        """
        if json_data['type'] in filter_set:
            return True
        else:
            return False

    # TODO: Move the following methods to a different file
    def _url2dictlist(self, mined_url: str) -> List[Dict]:
        """
        Generates a json file with all the metadata.

        Parameters
        ----------
        mined_url: str
            URL of the GH Archive json.gz file.

        Returns
        -------
        List(Dict):
            Returns a list of the JSON dictionaries corresponding to the mined data.
        """
        https = PoolManager(cert_reqs='CERT_REQUIRED',
                            ca_certs=certifi.where())
        dict_list = []
        try:
            response = https.request('GET', mined_url)
            compressed_json = BytesIO(response.data)
            with gzip.GzipFile(fileobj=compressed_json) as json_bytes:
                json_str = json_bytes.read().decode('utf-8')
                for json_value in json_str.split('\n'):
                    if self._is_valid_json(json_value):
                        data = json.loads(json_value)
                        if self.filter_by_event(data, self.event_set):
                            dict_list.append(data)
        except OSError:
            pass

        return dict_list

    def _events_dataframe(self) -> None:
        """
        Generate a DataFrame for all the mined attributes
        """

        for mined_url in self._daterange2url():
            logging.info(" CRAWLER: Processing {}".format(mined_url))

            key = mined_url[mined_url.rfind("/") + 1:].split(".")[0]
            all_events = self._url2dictlist(mined_url)
            mined_data_dict = defaultdict(lambda: defaultdict(int))

            for event in all_events:
                event_type = event['type']
                repo_name = event['repo']['name']
                mined_data_dict[event_type][repo_name] += 1

            mined_data_df = pd.DataFrame(mined_data_dict).fillna(0)
            mined_data_df['TotalEvents'] = mined_data_df.sum(axis=1)
            mined_data_df['NumEvents'] = mined_data_df.astype(
                bool).sum(axis=1) - 1
            mined_data_df.sort_values(
                by=['NumEvents', 'TotalEvents'], ascending=False, inplace=True)

            self.mined_data_df[key] = mined_data_df

    def get_events_as_dataframe(self) -> PandasDataFrame:
        """
        Generate a pandas file with all the mined attributes

        Returns
        -------
        DataFrame:
            All mined data as a pandas DataFrame
        """

        if len(self.mined_data_df) > 0:
            return self.mined_data_df

        self._events_dataframe()
        return self.mined_data_df

    def save_events_as_csv(self,
                           save_path: Path = root.joinpath('data', 'hourly')) -> None:
        """
        Generate a CSV file with all the mined attributes

        Parameters
        ----------
        save_path: str
            Save path as a string.
        """

        for fname, data_df in self.get_events_as_dataframe().items():
            logging.debug("[+] Processing file {}.csv".format(fname))
            save_location = save_path.joinpath(fname + '.csv')
            data_df.to_csv(save_location)

    def save_events_as_json(self,
                            save_path: Path = root.joinpath('data')) -> None:
        """
        Generate a JSON file with all the mined attributes

        Parameters
        ----------
        save_path: str
            Save path as a string.
        """

        for fname, data_df in self.get_events_as_dataframe().items():
            logging.debug("[+] Processing file {}.json".format(fname))
            save_location = save_path.joinpath(fname + '.json')
            data_df.to_json(save_location)
