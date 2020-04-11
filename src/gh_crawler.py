import requests
from typing import Dict, Tuple, List, Union, NewType
from itertools import product
from pdb import set_trace

DateRange = Tuple(int, int)
URL = NewType("URL", str)

class GHArchiveCrawler:
    def __init__(self, hour: Union(DateRange, int) = (0, 23),
            date: Union(DateRange, int) = (1, 31),
            month: Union(DateRange, int) = (1, 12),
            year: Union(DateRange, int) = (2019, 2020)):
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
            hh = hh,
        
        if isinstance(self.date, tuple):
            dd = dd,
        
        if isinstance(self.month, tuple):
            mm = mm,
        
        if isinstance(self.year, tuple):
            yy = yy, 
        
        all_timestamps = tuple(product(yy, mm, dd, hh))
        for y, m, d, h in all_timestamps:
            set_trace()
            yield "https://data.gharchive.org/{}-{}-{}-{}.json.gz".format(h, d, m, y)


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
    ghc = GHArchiveCrawler()
    ghc._daterange2url()
