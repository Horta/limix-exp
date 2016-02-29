from limix_exp.task import TaskResult
from numpy import asarray

class QTLTaskResult(TaskResult):
    __slots__  = ['_pv']

    def __init__(self, workspace_id, experiment_id, task_id):
        super(QTLTaskResult, self).__init__(workspace_id, experiment_id,
                                              task_id)
        self._pv = dict()

    def pv(self, method):
        return self._pv[method]

    def set_pv(self, method, pv):
        self._add_method(method)
        pv = asarray(pv, float)
        assert pv.ndim == 1
        self._pv[method] = pv
