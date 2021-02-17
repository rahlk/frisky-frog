import os
import sys
import json
from statistics import median
from pathlib import Path
from collections import defaultdict
from contextlib import ContextDecorator

# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))


if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))


class AdjacencyGraph(ContextDecorator):
    
    def __init__(self):
        pass


    def __enter__(self):
        # - Add data dir -
        self.data_path = root.joinpath('data')
        # - Add save dir -
        self.save_dir = self.data_path.joinpath("collaboration", "non-covid", "top-10-repos", "adjacency-matrices")
        # - Container to hold saved results -
        self.all_repos = defaultdict(None)
        return self


    def comments_date(self, data):
        """
        Return a dict of all comments made on a repo with keys as dates and values as arrays of comment_ids.

        Parameters
        ----------
        data: dict
            dict of a json_load(repo).

        Returns
        -------
        Dict[date]:
            A dict containing all the comments sorted by date.
        """
        #sort comments by date into date dictionary
        comments_by_date = {}

        for key, subdict in data.items():
            if 'date' in subdict.keys():
                date = subdict['date']
                if date in comments_by_date.keys():
                    comments_by_date[date].append(key)
                else:
                    comments_by_date[date] = [key]
        return comments_by_date


    def adjacency_matrix(self, data, comments_in_a_day):
        """
        Return a dict of an adjacency matrix of all comment_author/issue_author interactions of comments made in a day

        Parameters
        ----------
        data: dict
            dict of a json_load(repo)
        comments_in_a_day: array
            array of comment_ids of comments made in the same day

        Returns
        -------
        dict[Dict]:
            A dict with keys=authors and values=(dict with keys=authors and values=# of interactions)
        """
        
        #sort comments by date into date dictionary

        #get adjacency list and list of authors
        adjacency_list = {}
        authors = []
        for comment in comments_in_a_day:
            subdict = data[comment]
            comment_author = subdict['comment_author']
            issue_author = subdict['issue_author']

            #append to authors list
            if comment_author not in authors:
                authors.append(comment_author)
            if issue_author not in authors:
                authors.append(issue_author)

            #append to adjacency list
            if comment_author in adjacency_list.keys():
                if issue_author in adjacency_list[comment_author].keys():
                    adjacency_list[comment_author][issue_author] += 1
                else:
                    adjacency_list[comment_author][issue_author] = 1
            else:
                adjacency_list[comment_author] = {issue_author:1}

            #append again to adjacency matrix with issue_author first
            if issue_author in adjacency_list.keys():
                if comment_author in adjacency_list[issue_author].keys():
                    adjacency_list[issue_author][comment_author] += 1
                else:
                    adjacency_list[issue_author][comment_author] = 1
            else:
                adjacency_list[issue_author] = {comment_author:1}
        
        #calculate median_indegree
        indegrees = []
        for key in adjacency_list.keys():
            indegrees.append(len(adjacency_list[key]))
        median_indegree = median(indegrees)

        adjacency_matrix = {}
        adjacency_matrix['MID'] = round(median_indegree/len(authors), 3)
        #fill in adjacency matrix with 0's
        for i in authors:
            adjacency_matrix[i] = {}
            for j in authors:
                if j not in adjacency_list[i].keys():
                    adjacency_matrix[i][j] = 0
                else:
                    adjacency_matrix[i][j] = adjacency_list[i][j]
                
        
        return adjacency_matrix


    def __call__(self):
        #iterate through all json files in collaboration/covid directory
        os.chdir('/local/jyh2131/frisky-frog/data/collaboration/non-covid/top-10-repos/comments')

        fileList = []

        for root, dirs, files in os.walk('/local/jyh2131/frisky-frog/data/collaboration/non-covid/top-10-repos/comments'):
            
            for name in files:
                fileList.append(os.path.join(root, name))
            
        for file in fileList:
            
            with open(file, 'r') as currentFile:
                data = json.load(currentFile)

                adjacency_matrices = {}

                #returns a dict with keys as dates and values as array of comment ids
                comments_by_date = self.comments_date(data)
                for key, subarray in comments_by_date.items():
                    adjacency_matrices[key] = self.adjacency_matrix(data, subarray)
        
                repo_name = file.rsplit('/', 1)[1]
                
                self.all_repos[repo_name] = adjacency_matrices

    def __exit__(self, exc, value, traceback):
        for repo, matrices_dict in self.all_repos.items():
            
            save_name = self.save_dir.joinpath('{}-{}'.format('AM', repo))
            with open(save_name, 'w+') as json_save_path:
                print(json.dumps(matrices_dict, indent=2), file=json_save_path)      


if __name__ == "__main__":
    with AdjacencyGraph() as adjacency_graph:
        adjacency_graph()

