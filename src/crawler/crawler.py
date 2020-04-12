import requests
import certifi
from typing import Dict, Tuple, List, Union, NewType
from itertools import product
from pdb import set_trace
from io import BytesIO
import json
import gzip
from urllib3 import PoolManager

DateRange = Tuple[int, int]
URL = NewType("URL", str)


class Crawler:
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

    Notes
    -----    
    + For specific values use Discrete numbers.
    E.g., for April, 20th 2019, 4:00 PM provide: hour=16, date=20, month=4, year=2019.

    + For ranges provide a tuple of start and end values
    E.g., for first half of 2019 provide: hour=(0, 23), date=(1, 31), month=(1, 6), year=2019.
    """

    def __init__(self, hour: Union[DateRange, int] = (0, 23),
                 date: Union[DateRange, int] = (1, 31),
                 month: Union[DateRange, int] = (1, 12),
                 year: Union[DateRange, int] = (2019, 2020)):

        self.date = date
        self.year = year
        self.hour = hour
        self.month = month

    @staticmethod
    def _inclusive_range(start, end):
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
        Converts user provided date range into a GH Archive URL.

        GH Archive uses a specific string format to encode the data url. This method formats user provided date-range into a URL that can be queried. 

        Yields
        ------
        str:
            Download URL for the GH Archive data
        """

        # If no range is provided, convert to a single element tuple. It's just
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

        # Generate a cartesian product of all the possible date-time
        # combinations
        all_timestamps = tuple(product(year, month, date, hour))

        for yy, mm, dd, hh in all_timestamps:
            yield "https://data.gharchive.org/{:02d}-{:02d}-{:02d}-{}.json.gz".format(yy, mm, dd, hh)

    def _url2dictlist(self) -> List[Dict]:
        """
        Generates a json file with all the metadata.

        Returns
        -------
        List(Dict):
            Returns a list of the JSON dictionaries corresponding to the mined data.
        """
        https = PoolManager(cert_reqs='CERT_REQUIRED',
                            ca_certs=certifi.where())
        dict_list = []
        for mined_url in self._daterange2url():
            response = https.request('GET', mined_url)
            compressed_json = BytesIO(response.data)
            with gzip.GzipFile(fileobj=compressed_json) as json_bytes:
                json_str = json_bytes.read().decode('utf-8')
                for json_value in json_str.split("\n"):
                    if self._is_valid_json(json_value):
                        data = json.loads(json_value)
                        dict_list.append(data)

        return dict_list

    def get_events_as_dataframe(self):
        """
        Generate a DataFrame for all the mined attributes
        """

    def save_events_as_csv(self):
        """
        Generate a CSV file with all the mined attributes
        """

    def save_events_as_json(self):
        """
        Generate a JSON file with all the mined attributes
        """


if __name__ == "__main__":
    crawler = Crawler()
    crawler._url2dictlist()