import workspace

def get_workspace(workspace_id):
    return workspace.get_workspace(workspace_id)

def get_experiment(workspace_id, experiment_id):
    return workspace.get_workspace(workspace_id).get_experiment(experiment_id)
