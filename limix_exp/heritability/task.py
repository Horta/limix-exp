from limix_exp.task import TaskResult
from numpy import asarray, asscalar

class H2TaskResult(TaskResult):
    __slots__  = ['_h2']

    """docstring for H2TaskResult"""
    def __init__(self, workspace_id, experiment_id, task_id):
        super(H2TaskResult, self).__init__(workspace_id, experiment_id,
                                           task_id)

        self._h2 = dict()

    def h2_err(self, method):
        return self.h2(method) - self.get_task().h2

    def h2(self, method):
        h2 = asarray(self._h2[method], float)
        h2 = asscalar(h2)
        return h2

    def set_h2(self, method, h2):
        self._add_method(method)
        h2 = asarray(h2, float)
        self._h2[method] = asscalar(h2)
