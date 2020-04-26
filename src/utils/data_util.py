import re
import json
import pandas as pd
from pathlib import Path
from ipdb import set_trace
from collections import defaultdict


def json2repos(json_dir: Path, save_dir: Path) -> None:
    repos = defaultdict(list)
    for json_file in json_dir.glob("*.json"):
        with open(json_file) as f:
            data = json.load(f)
            for repo, metrics in data.items():
                dframe = pd.DataFrame.from_dict(metrics, orient='index')
                repos[repo].append(dframe)

    for repo, all_metrics in repos.items():
        repo_name = re.sub('/', '-', repo)
        dframe = pd.concat(all_metrics, sort=True)
        dframe.index = pd.to_datetime(dframe.index)
        dframe = dframe.sort_index()

        dframe.to_csv(save_dir.joinpath("{}.csv".format(repo_name)),
                      index_label='Date')
