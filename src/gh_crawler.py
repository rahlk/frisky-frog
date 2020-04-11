import requests

class GHArchiveCrawler:
    def __init__(self, hour=(0, 23), day=(1, 31), month=(1, 12), year=(2019, 2020)):
        """
        A crawler for GH Archive (https://www.gharchive.org/).

        Query GH Archive and covert data in to a format that can be processed with pandas, csv, etc.

        Parameters
        ----------
        hour: Tuple(Int, Int) (default=(0, 23))
            Hourly data as a tuple of start and end values.
        day: Tuple(Int, Int) (default=(1, 31))
            Daily data as a tuple of start and end values.
        month: Tuple(Int, Int) (default=(1, 12))
            Monthly data as a tuple of start and end values.
        year: Tuple(Int, Int) (default=(2019, 2020))
            Yearly data as a tuple of start and end values.

        Notes
        -----    
        + For specific values use Discrete numbers.
        E.g., for April, 20th 2019, 4:00 AM -- 5:00 AM provide: hour=4, day=20, month=4, year=2019.

        + For ranges provide a tuple of start and end values
        E.g., for first half of 2019 provide: hour=(0, 23), day=(1, 31), month=(1, 6), year=2019.
        """

        self.day = day
        self.year = year
        self.hour = hour
        self.month = month


    def _daterange2url():
        """
        Converts user provided time into a github archive url.

        Parameters
        ----------

        Returns
        -------

        """

    def _url2json(gh_archive_string):
        """
        Generates a json file with all the metadata
        """

    def get_events_as_csv():
        """

        """

    def get_events_as_json():
        """

        """
    
    def get_events_as_dataframe():
        """

        """
    
if __name__ == "__main__":
    
