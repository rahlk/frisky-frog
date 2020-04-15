import os
import sys
import json
import logging
import pandas as pd
from pdb import set_trace
from datetime import datetime
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
PathType = NewType('Path', PosixPath)


class Agglomerate:
    def __init__(self, data_path=root.joinpath('data')):
        """
        Agglomerates hourly GH Archive data into daily, weekly, and monthly data.

        Parameters
        ----------
        data_path: PathType (default: {root}/data/)
            Path to data. 
        """
        self.data_path = data_path

    def hourly2daily(self) -> PandasDataFrame:
        """
        Agglomerates hourly to daily.

        Returns
        -------
        PandasDataFrame
            Daily events count.
        """
        for hourly_data in self.data_path.joinpath('hourly').glob('*.csv'):
            dframe = pd.read_csv(hourly_data)
            fname = hourly_data.stem
            set_trace()

    def hourly2weekly(self) -> PandasDataFrame:
        """
        Agglomerates hourly to daily.

        Returns
        -------
        PandasDataFrame
            Weekly events count.
        """

    def hourly2monthly(self) -> PandasDataFrame:
        """
        Agglomerates hourly to daily.

        Returns
        -------
        PandasDataFrame
            Monthly events count.
        """

    def agglomerate_all(self,
                        also_save: bool = False,
                        save_path: PathType = root.joinpath('data'),
                        file_extn: str = 'csv') -> Dict[str, PandasDataFrame]:
        """
        Agglomerates hourly to all other ranges.  

        Parameters
        ----------
        also_save: bool (default: False)
            (Optional) Determines if the dataframe should be saved.
        save_path: PathType (default: {root}/data/)
            (Optional) Path to save file.
        file_extn: str (default: csv)
            (Optional) Extension of save file. 

        Returns
        -------
        Dict[PandasDataFrame]
            A dictionary of dataframes.
        """


if __name__ == "__main__":
    agg = Agglomerate()
    daily = agg.hourly2daily()
