import os
import sys
import json
import logging
import pandas as pd
from ipdb import set_trace
from datetime import datetime
from pathlib import Path, PosixPath
from collections import defaultdict
from typing import Dict, Tuple, List, Union, NewType
from tqdm import tqdm

# Logging Config
logging.basicConfig(format='[++] %(message)s', level=logging.INFO)

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
PathType = NewType('Path', PosixPath)
DateTime = NewType('DateTime', datetime)


class Agglomerate:
    def __init__(self, data_path=root.joinpath('data')):
        """
        Agglomerates hourly GH Archive data into daily, weekly, and monthly data.

        Parameters
        ----------
        data_path: PathType (default: {root}/data/)
            Path to data.
        match_string: str
            Date to match. The string is formatted as YYYY-MM.
        """

        self.data_path = data_path

    def set_match_string(self, match_string: str):
        self.date_match = match_string

    @staticmethod
    def _datetime_to_key(timestamp: DateTime, granularity: str) -> str:
        """
        Converts a given datetime object into a string key.

        The string key is inturn used to construct dicttionaries of data.

        Parameters
        ----------
        DateTime: str
            The timestamp object.
        granularity: str
            The datetime granularity to reprsent.

        Returns
        -------
        str:
            The dictionary key.
        """
        assert granularity.lower() in {
            'daily', 'weekly', 'monthly'}, "Chosen granularity not 'daily', 'weekly', or 'montly'. Please choose from among these."

        if granularity.lower() == "daily":
            return timestamp.strftime("%Y-%m-%d")

        if granularity.lower() == "weekly":
            return timestamp.strftime("%Y-%W")

        if granularity.lower() == "monthly":
            return timestamp.strftime("%Y-%m")

    def hourly2daily(self, also_save: bool = True) -> PandasDataFrame:
        """
        Agglomerates hourly to daily.

        Parameters
        ----------
        also_save: bool
            A flag to save the agglomerated data.

        Returns
        -------
        PandasDataFrame
            Daily events count.
        """
        unmerged = defaultdict(list)
        pddframe = defaultdict(lambda: None)

        for hourly_data in tqdm(self.data_path.joinpath('hourly').glob('*.csv'), desc='[+] Agglomerator: Gathering hourly Data.'):
            fname = hourly_data.stem
            try:
                datetime_obj = datetime.strptime(fname, "%Y-%m-%d-%H")
            except ValueError:  # This catches and ignores invalid dates.
                continue
            key = self._datetime_to_key(datetime_obj, granularity='daily')
            if self.date_match is not None and self.date_match not in key:
                continue
            else:
                dframe = pd.read_csv(hourly_data, index_col=0)
                unmerged[key].append(dframe)

        for day, df_collection in tqdm(unmerged.items(), desc="[+] Agglomerator: Aggregating hourly Data."):
            coarser_df = df_collection[0]
            for i, next_df in enumerate(df_collection[1:]):
                coarser_df = coarser_df.add(next_df, fill_value=0)

            num_events = coarser_df.astype(bool).sum(axis=1)
            total_events = coarser_df.sum(axis=1)

            coarser_df["TotalEvents"] = total_events
            coarser_df["NumEvents"] = num_events

            coarser_df = coarser_df.sort_values(by=['NumEvents', 'TotalEvents'], ascending=False)

            if also_save:
                coarser_df.to_csv(root.joinpath(
                    'data/daily/{}.csv'.format(day)), index_label="Repositories")
            pddframe[day] = coarser_df

        return pddframe

    def hourly2weekly(self, also_save: bool = True) -> PandasDataFrame:
        """
        Agglomerates hourly to daily.

        Parameters
        ----------
        also_save: bool
            A flag to save the agglomerated data.

        Returns
        -------
        PandasDataFrame
            Weekly events count.
        """
        unmerged = defaultdict(list)
        pddframe = defaultdict(lambda: None)

        for hourly_data in self.data_path.joinpath('hourly').glob('*.csv'):
            fname = hourly_data.stem
            try:
                datetime_obj = datetime.strptime(fname, "%Y-%m-%d-%H")
            except ValueError:  # This catches and ignores invalid dates.
                continue
            key = self._datetime_to_key(datetime_obj, granularity='weekly')
            if self.date_match is not None and datetime.strptime(
                    datetime_obj, "%Y-%m") != datetime.strptime(
                    self.date_match, "%Y-%m"):
                continue
            else:
                dframe = pd.read_csv(hourly_data, index_col=0)
                unmerged[key].append(dframe)

        for week, df_collection in unmerged.items():
            logging.info(
                " AGGLOMERATE: Processing hourly-to-week for {}".format(week))
            coarser_df = df_collection[0]
            for i, next_df in enumerate(df_collection[1:]):
                coarser_df = coarser_df.add(next_df, fill_value=0)

            num_events = coarser_df.astype(bool).sum(axis=1)
            total_events = coarser_df.sum(axis=1)

            coarser_df["TotalEvents"] = total_events
            coarser_df["NumEvents"] = num_events

            coarser_df.sort_values(
                by=['NumEvents', 'TotalEvents'], ascending=False, inplace=True)

            if also_save:
                coarser_df.to_csv(root.joinpath(
                    'data/weekly/{}.csv'.format(week)), index_label="Repositories")

            pddframe[week] = coarser_df

        return pddframe

    def hourly2monthly(self, also_save: bool = True) -> PandasDataFrame:
        """
        Agglomerates hourly to daily.

        Parameters
        ----------
        also_save: bool
            A flag to save the agglomerated data.

        Returns
        -------
        PandasDataFrame
            Monthly events count.
        """
        unmerged = defaultdict(list)
        pddframe = defaultdict(lambda: None)

        for hourly_data in self.data_path.joinpath('hourly').glob('*.csv'):
            fname = hourly_data.stem
            try:
                datetime_obj = datetime.strptime(fname, "%Y-%m-%d-%H")
            except ValueError:  # This catches and ignores invalid date ranges.
                continue
            key = self._datetime_to_key(datetime_obj, granularity='monthly')
            if self.date_match is not None and key != self.date_match:
                continue
            else:
                dframe = pd.read_csv(hourly_data, index_col=0)
                unmerged[key].append(dframe)

        for month, df_collection in unmerged.items():
            logging.info(
                " AGGLOMERATE: Processing hourly-to-monthy for {}".format(month))
            coarser_df = df_collection[0]
            for i, next_df in enumerate(df_collection[1:]):
                coarser_df = coarser_df.add(next_df, fill_value=0)

            num_events = coarser_df.astype(bool).sum(axis=1)
            total_events = coarser_df.sum(axis=1)

            coarser_df["TotalEvents"] = total_events
            coarser_df["NumEvents"] = num_events

            coarser_df.sort_values(
                by=['NumEvents', 'TotalEvents'], ascending=False, inplace=True)

            if also_save:
                coarser_df.to_csv(root.joinpath(
                    'data/monthly/{}.csv'.format(month)), index_label="Repositories")
            pddframe[month] = coarser_df

        return pddframe

    def alltime(self):
        """
        Agglomerate all time top repos.
        """
        # unmerged = defaultdict(list)
        # pddframe = defaultdict(lambda: None)

        for i, monthly_data in enumerate(self.data_path.joinpath('monthly').glob('*.csv')):
            monthly = pd.read_csv(monthly_data, index_col=0)
            monthly = monthly.iloc[:1000000]
            monthly.drop([
                'CommitCommentEvent', 'ForkEvent', 'IssuesEvent', 'PullRequestEvent', 'PullRequestReviewCommentEvent', 'PushEvent'], axis=1, inplace=True)

            if i == 0:
                ranking = monthly
            else:
                ranking = ranking.add(monthly, fill_value=0)

        set_trace()


if __name__ == "__main__":
    agg = Agglomerate()
    agg.alltime()
