import argparse
import json
import os
from datetime import datetime
from datetime import timedelta
import numpy as np


def sort_commits(all_commits):
    return sorted(all_commits,
                  key=lambda x: datetime.fromtimestamp(x['timestamp']))


def read_data(data_dir):
    author_file = open(os.path.join(data_dir, 'authors.json'))
    paths_file = open(os.path.join(data_dir, 'files.json'))
    commits_file = open(os.path.join(data_dir, 'commits.json'))
    return json.load(author_file), \
           json.load(paths_file), \
           sort_commits(json.load(commits_file))


def divide_commits_into_days(all_commits):
    day_to_commit = {}
    for commit in all_commits:
        t = datetime.fromtimestamp(commit['timestamp'])
        d = t.date()
        if d not in day_to_commit.keys():
            day_to_commit[d] = []
        day_to_commit[d].append(commit)
    return day_to_commit


def divide_into_interaction_chunks(all_commits, window_size=1, stride=1, ignore_empty_days=True):
    daily_commits = divide_commits_into_days(all_commits)
    days = sorted(list(daily_commits.keys()))
    first_day = days[0]
    last_day = days[-1]
    day_chunks = []
    while first_day < last_day:
        next_day = first_day + timedelta(days=window_size-1)
        commits_in_this_chunk = []
        for day in daily_commits.keys():
            if first_day <= day <= next_day:
                commits_in_this_chunk.extend(daily_commits[day])
        if not ignore_empty_days or len(commits_in_this_chunk) > 0:
            window_chunk = {
                'first_day': first_day,
                'last_day': next_day,
                'commits': commits_in_this_chunk
            }
            day_chunks.append(window_chunk)
        first_day = first_day + timedelta(days=stride-1)
    return day_chunks


def author_activity(authors, commits):
    activities = np.zeros(len(authors), dtype='int')
    for c in commits:
        activities[c['author_id']] += 1
    return activities.tolist()


def create_author_interaction_graph(authors, commits):
    _author_interaction_graph = np.zeros(shape=(len(authors), len(authors)), dtype='int')
    assert np.sum(_author_interaction_graph) == 0
    modified_files = dict()
    for c in commits:
        for f in c['files']:
            if f not in modified_files:
                modified_files[f] = set()
            modified_files[f].add(c['author_id'])
    for f in modified_files:
        interacted_authors = set(modified_files[f])
        if len(interacted_authors) > 1:
            for a1 in interacted_authors:
                for a2 in interacted_authors:
                    if a1 != a2:
                        _author_interaction_graph[a1, a2] += 1
    return _author_interaction_graph

## Call this function with appropriate parameters extracted from gather commit file.
def create_graph(_commits, _authors, save=True, exp_name=None, save_file_dir=None, sliding_window_size=1):
    interaction_chunks = divide_into_interaction_chunks(
        all_commits=_commits,
        window_size=sliding_window_size,
        stride=2
    )
    all_interaction_graphs = [
        {
            'first_day': str(chunk['first_day']),
            'last_day': str(chunk['last_day']),
            'interaction': create_author_interaction_graph(
                _authors, chunk['commits']
            ).tolist(),
            'author_activities': author_activity(_authors, chunk['commits'])
        } for chunk in interaction_chunks
    ]
    if save:
        assert save_file_dir is not None, 'Save Directory must be provided to save results'
        assert exp_name is not None, 'Experiment name must be provided to save the results'
        if not os.path.exists(save_file_dir):
            os.mkdir(save_file_dir)
        save_file = open(os.path.join(save_file_dir, exp_name + '.json'), 'w')
        json.dump(all_interaction_graphs, save_file)
        save_file.close()
    return all_interaction_graphs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', help='Data Directory', type=str, default='commit_data/nodejs_i18n')
    parser.add_argument('--sliding_window_size',
                        help='Number of days to consider interaction', type=int,
                        default=1)
    parser.add_argument('--overlap_windows',
                        help='Overlap the windows', action='store_true')
    parser.add_argument('--stride',
                        help='Stride between windows', type=int, default=1)
    parser.add_argument('--save',
                        help='Path for saving result',
                        default='code_author_interaction_graphs')
    parser.add_argument(
        '--name', help='Name of this experiment', default=None)
    args = parser.parse_args()
    authors, files, commits = read_data(args.data)
    stride = args.stride
    stride += args.sliding_window_size if not args.overlap_windows else 0
    exp_name = args.data.split('/')[-1].strip() \
            if '/' in args.data else args.data.strip()
    save_file_dir = args.save
    graphs = create_graph(commits, authors, save=True, save_file_dir=save_file_dir, exp_name=exp_name)
    print(len(graphs))
    pass
