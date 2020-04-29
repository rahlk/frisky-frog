import os
import sys
import json
import pandas
import logging
from pathlib import Path
from ipdb import set_trace
from datetime import datetime
from collections import defaultdict

# -- Update paths --
# - Logging Config -
logging.basicConfig(format='[+] %(message)s', level=logging.INFO)

# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

# - Add data dir to path -
data = root.joinpath('data')


if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))


def measure_json2table():
    result_dict = defaultdict(int)
    for file in data.joinpath('measures', 'repositories', 'all').glob('*.json'):
        with open(file, "r") as json_file:
            try:
                json_str = json_file.read()
                repos_created_dict = json.loads(json_str)
                date = datetime.strptime(file.stem, "%Y-%m-%d")
                count_repos_created = repos_created_dict['total_count']
                result_dict[date] = count_repos_created
            except:
                set_trace()
        
    set_trace()


if __name__ == "__main__":
    measure_json2table()
