from limix_exp.base import BasePlot
from limix_tool.qtl import NullScore
from limix_tool.qtl import combine_pvalues
# from gwarped_exp import workspace
from limix_plot.qqplot import QQPlot

class CalPlot(BasePlot):
    def __init__(self, tasks, group_by=None):
        super(CalPlot, self).__init__(tasks, group_by)

        if len(tasks) > 0:
            self._workspace = workspace.get_workspace(tasks[0]._workspace_id)
        else:
            self._workspace = None

    def _plot_tasks(self, tasks, ax):
        w = workspace.get_workspace(tasks[0]._workspace_id)
        properties = w.get_properties()

        methods = tasks[0].get_result().methods

        nfails = {m:0 for m in methods}

        ns = NullScore()

        for method in methods:
            pvs = [task.get_result().pv(method) for task in tasks]
            pv = combine_pvalues(pvs)
            ns.add(method, pv)

        qqplot = QQPlot()
        for method in methods:
            qqplot.add(ns.pv(method), method)
            confid = ns.confidence_band(method)
            qqplot.set_confidence(confid, method)

        qqplot.plot_top = 10.
        qqplot.plot(ax)
        ax.set_xlim([0, 20])
        ax.set_ylim([0, 20])
