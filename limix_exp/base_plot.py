import limix_util as lu
import matplotlib.pylab as plt
from limix_util.misc import grid_plot

def _sort_key(k):
    if lu.isfloat(k):
        return float(k)
    return k

class BasePlot(object):
    def __init__(self, tasks, group_by=None):
        if group_by:
            group_by = group_by if isinstance(group_by, list) else [group_by]
            self._tasks = lu.group_by(tasks, group_by, sort_key=_sort_key)
        else:
            self._tasks = tasks
        self._group_by = group_by

    def plot(self):
        fig = plt.figure()
        if self._group_by:
            grid_plot(self._group_by, self._tasks,
                      self._plot_tasks, fig=fig)
        else:
            axes = fig.add_subplot(111)
            self._plot_tasks(self._tasks, axes)

    def plot_grouped(self):
        fig = plt.figure()
        if self._group_by:

            def callback(tasks, axis):
                self._plot_grouped_tasks(self._group_by[-1], tasks, axis)

            grid_plot(self._group_by[:-1], self._tasks,
                      callback, fig=fig)
        else:
            axes = fig.add_subplot(111)
            self._plot_tasks(self._tasks, axes)

    def _plot_tasks(self, tasks, axis):
        raise NotImplementedError

    def _plot_grouped_tasks(self, group_name, tasks, axis):
        raise NotImplementedError
