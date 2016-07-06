from matplotlib.pyplot import figure
from limix_util.scalar import isfloat
from limix_util.dict import group_by as lms_group_by
from limix_plot.grid import grid_plot

def _sort_key(k):
    if isfloat(k):
        return float(k)
    return k

class BasePlot(object):
    def __init__(self, fig=None):
        self.tasks = []
        self.group_by = None
        self._fig = fig

    def _setup_tasks_grouping(self):
        if self.group_by is not None:
            self.tasks = lms_group_by(self.tasks, self.group_by,
                                     sort_key=_sort_key)

    def plot(self):
        self._setup_tasks_grouping()

        fig = figure() if self._fig is None else self._fig
        if self.group_by:
            grid_plot(self.group_by, self.tasks,
                      self._plot_tasks, fig=fig)
        else:
            axes = fig.add_subplot(111)
            self._plot_tasks(self.tasks, axes)

    def plot_grouped(self):
        self._setup_tasks_grouping()

        fig = figure() if self._fig is None else self._fig
        if self.group_by:

            def callback(tasks, axis):
                self._plot_grouped_tasks(self.group_by[-1], tasks, axis)

            grid_plot(self.group_by[:-1], self.tasks,
                      callback, fig=fig)
        else:
            axes = fig.add_subplot(111)
            self._plot_tasks(self.tasks, axes)

    def _plot_tasks(self, tasks, axis):
        raise NotImplementedError

    def _plot_grouped_tasks(self, group_name, tasks, axis):
        raise NotImplementedError
