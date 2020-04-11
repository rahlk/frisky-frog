import requests
from typing import Dict, Tuple, List, Union, NewType

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

    def _daterange2url(self) -> URL:
        """
        Converts user provided date range into a GH Archive URL.

        Returns
        -------
        str:
            Download URL for the GH Archive data
        """
        
        hh = "{:02d}".format(self.hour)
        dd = "{:02d}".format(self.date)
        mm = "{:02d}".format(self.month)
        yy = "{:04d}".format(self.year)

        if isinstance(self.hour, tuple):
            hh = "{{{:02d}..{:02d}}}".format(*self.hour)
        
        if isinstance(self.date, tuple):
            dd = "{{{:02d}..{:02d}}}".format(*self.date)
        
        if isinstance(self.month, tuple):
            mm = "{{{:02d}..{:02d}}}".format(*self.month)
        
        if isinstance(self.year, tuple):
            yy = "{{{:04d}..{:04d}}}".format(*self.year)
        
        url_string = "https://data.gharchive.org/{}-{}-{}-{}.json.gz".format(hh, dd, mm, yy)

        return url_string

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

        """

    def save_events_as_csv(self):
        """
        Generate a CSV file with all the events
        """

    def save_events_as_json(self):
        """

        """
    
    
if __name__ == "__main__":
    
