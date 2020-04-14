from commit_graph.create_graph import create_graph
from commit_graph.gather_commit import download_commit_summaries, debug


def generate_graphs(owner, repo_name, time_back=14):
    commits, authors, files = download_commit_summaries(
        repo_owner_name=owner,
        repo_name=repo_name,
        time_budget=time_back,
        include_merge_commit=False,
        delete_repo_after_finish=True
    )

    graphs = create_graph(
        _commits=commits, _authors=authors, save=True,
        save_file_dir='commit_interaction_graph', exp_name=owner_name + '_' + repo_name
    )
    return graphs


if __name__ == '__main__':
    owner_name = 'nodejs'
    repo = 'i18n'
    graphs = generate_graphs(owner_name, repo)
    debug(len(graphs))
