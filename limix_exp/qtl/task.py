from limix_exp.task import TaskResult
from numpy import asarray

class QTLTaskResult(TaskResult):
    __slots__  = ['_pv', '_stats']

    def __init__(self, workspace_id, experiment_id, task_id):
        super(QTLTaskResult, self).__init__(workspace_id, experiment_id,
                                              task_id)
        self._pv = dict()
        self._stats = dict()

    def pv(self, method):
        return self._pv[method]

    def set_pv(self, method, pv):
        self._add_method(method)
        pv = asarray(pv, float)
        assert pv.ndim == 1
        self._pv[method] = pv

    def stats(self, method):
        return self._stats[method]

    def set_stats(self, method, stats):
        self._add_method(method)
        stats = asarray(stats, float)
        assert stats.ndim == 1
        self._stats[method] = stats
