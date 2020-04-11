import requests
from typing import Dict, Tuple, List, Union, NewType
from itertools import product
from pdb import set_trace

DateRange = Tuple[int, int]
URL = NewType("URL", str)

class GHArchiveCrawler:
    def __init__(self, hour: Union[DateRange, int] = (0, 23),
            date: Union[DateRange, int] = (1, 31),
            month: Union[DateRange, int] = (1, 12),
            year: Union[DateRange, int] = (2019, 2020)):
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

        self.date = date
        self.year = year
        self.hour = hour
        self.month = month
        self.range_requested: bool = True

    def _daterange2url(self) -> URL:
        """
        Converts user provided date range into a GH Archive URL.

        Yields
        ------
        str:
            Download URL for the GH Archive data
        """
        
        # Format ranges into appropriate string.
        if not isinstance(self.hour, tuple):
            self.hour = self.hour,
        
        if not isinstance(self.date, tuple):
            self.date = self.date,
        
        if not isinstance(self.month, tuple):
            self.month = self.month,
        
        if not isinstance(self.year, tuple):
            self.year = self.year,
        
        all_timestamps = tuple(product(self.year, self.month, self.date, self.hour))
        
        for yy, mm, dd, hh in all_timestamps:
            yield "https://data.gharchive.org/{:02d}-{:02d}-{:02d}-{}.json.gz".format(yy, mm, dd, hh)


    def _url2json(self, gh_archive_url: URL) -> Dict:
        """
        Generates a json file with all the metadata.

        Paramters
        ---------
        gh_archive_url: URL 
            The URL of the hourly archive

        Returns
        -------

        """

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
    crawler = GHArchiveCrawler()
    ghc._daterange2url()
