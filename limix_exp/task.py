import workspace
from numpy import asarray, asscalar
import limix_util as lu
from limix_util.pickle_ import SlotPickleMixin
from limix_util.misc import BeginEnd
import os
from tabulate import tabulate

def extract_successes_and_failures(tasks):
    methods = tasks[0].get_result().methods

    nsucs = {m:0 for m in methods}
    nfails = {m:0 for m in methods}

    for task in tasks:
        tr = task.get_result()
        for m in methods:
            if tr.error_status(m) == 0:
                nsucs[m] += 1
            else:
                nfails[m] += 1

    return dict(nsucs=nsucs, nfails=nfails)

def extract_window_scores(tasks, window_size, score_approach='rank',
                          pvalue_correction=None):
    from score import WindowScore
    if len(tasks) == 0:
        return dict()

    methods = tasks[0].get_result().methods

    scores = {m:[] for m in methods}
    for task in tasks:
        tr = task.get_result()
        cm = tr.get_causal_markers()
        cp = tr.get_marker_pos()

        score = WindowScore(cm, cp, wsize=window_size, verbose=False)

        for m in methods:
            if tr.error_status(m) == 0:
                pv = tr.pv(m)
                if pvalue_correction is not None:
                    pv = pvalue_correction.correct(pv)
                score.add(m, pv)

        for m in methods:
            if tr.error_status(m) == 0:
                s = getattr(score, score_approach + '_score')(m)
                scores[m].append(s)

    return scores

class TaskArgs(SlotPickleMixin):
    """docstring for TaskArgs"""
    def __init__(self):
        super(TaskArgs, self).__init__()
        self._names = []

    def add(self, name):
        self._names.append(name)

    def get_names(self):
        return self._names

class Task(object):
    """docstring for Task"""
    def __init__(self, workspace_id, experiment_id, task_id):
        super(Task, self).__init__()
        self.task_id = int(task_id)
        self.dataset = None
        self._workspace_id = workspace_id
        self._experiment_id = experiment_id

    def run(self):
        e = workspace.get_experiment(self._workspace_id, self._experiment_id)
        return e.do_task(self)

    def __getstate__(self):
        d = self.__dict__.copy()
        d['dataset_cls'] = self.dataset.__class__.__name__
        d['dataset_id'] = self.dataset.dataset_id
        del d['dataset']
        return d

    def __setstate__(self, d):
        import dataset

        dataset_cls = d['dataset_cls']
        dataset_id = d['dataset_id']

        dataset_cls = getattr(dataset, dataset_cls)
        dataset = dataset_cls(d['_workspace_id'], dataset_id)
        d['dataset'] = dataset

        del d['dataset_cls']
        del d['dataset_id']

        self.__dict__.update(d)

    @property
    def finished(self):
        return self.get_result() is not None

    def get_result(self):
        e = workspace.get_experiment(self._workspace_id, self._experiment_id)
        if e is None:
            return None

        return e.get_task_result(self.task_id)

class TaskResult(SlotPickleMixin):
    __slots__ = ['total_elapsed', '_workspace_id', '_experiment_id', 'task_id',
                 '_elapsed', '_error_status', '_error_msg', '_methods']

    """docstring for TaskResult"""
    def __init__(self, workspace_id, experiment_id, task_id):
        super(TaskResult, self).__init__()
        self.total_elapsed = float('nan')
        self._workspace_id = workspace_id
        self._experiment_id = experiment_id
        self.task_id = int(task_id)
        self._elapsed = dict()
        self._error_status = dict()
        self._error_msg = dict()
        self._methods = set()

    def get_task(self):
        import gwarped_exp as gwe
        e = gwe.get_experiment(self._workspace_id, self._experiment_id)
        return e.get_task(self.task_id)

    def elapsed(self, method):
        return self._elapsed[method]

    def error_status(self, method):
        return self._error_status[method]

    def error_msg(self, method):
        return self._error_msg[method]

    @property
    def methods(self):
        return list(self._methods)

    def set_error_status(self, method, error_status):
        self._add_method(method)
        self._error_status[method] = int(error_status)

    def set_error_msg(self, method, error_msg):
        self._add_method(method)
        self._error_msg[method] = str(error_msg)

    def set_elapsed(self, method, elapsed):
        self._add_method(method)
        self._elapsed[method] = float(elapsed)

    def _add_method(self, method):
        self._methods.add(method)


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


class PowerTaskResult(TaskResult):
    __slots__  = ['_pv']

    """docstring for PowerTaskResult"""
    def __init__(self, workspace_id, experiment_id, task_id):
        super(PowerTaskResult, self).__init__(workspace_id, experiment_id,
                                              task_id)
        self._pv = dict()

    def pv(self, method):
        return self._pv[method]

    def set_pv(self, method, pv):
        self._add_method(method)
        pv = asarray(pv, float)
        assert pv.ndim == 1
        self._pv[method] = pv

    # def __str__(self):
    #     from humanfriendly import format_timespan
    #     from tabulate import tabulate
    #
    #     msg = ''
    #     cm = lu.str_.array2string(self.get_causal_markers())
    #     table = [['Causal markers', cm]]
    #     mp = lu.str_.array2string(self.get_marker_pos())
    #     table.append(['Marker positions', mp])
    #     msg += tabulate(table) + '\n'
    #
    #
    #     for method in self.methods:
    #         table = []
    #         table.append(['Method', method])
    #         table.append(['Elapsed', format_timespan(self.elapsed(method))])
    #         table.append(['LML0', str(self.null_lml(method))])
    #         pv = asarray(self.pv(method))
    #         table.append(['Pvals', lu.str_.array2string(pv)])
    #         table.append(['Error status', str(self.error_status(method))])
    #         table.append(['Error message', str(self.error_msg(method))])
    #         msg += tabulate(table) + '\n'
    #
    #     return msg


# class CalTaskResult(TaskResult):
#     __slots__  = ['_y', '_ntrials', '_suc', '_pv', '_null_lml', '_beta_snp',
#                   '_elapsed', '_error_status', '_error_msg', '_causal_markers',
#                   '_marker_pos']
#
#     """docstring for CalTaskResult"""
#     def __init__(self, workspace_id, experiment_id, task_id):
#         super(CalTaskResult, self).__init__(workspace_id, experiment_id,
#                                               task_id)
#
#         self._y = None
#         self._ntrials = None
#         self._suc = None
#         self._pv = dict()
#         self._null_lml = dict()
#         self._beta_snp = dict()
#         self._elapsed = dict()
#         self._error_status = dict()
#         self._error_msg = dict()
#         self._causal_markers = dict()
#         self._marker_pos = dict()
#
#     def get_causal_markers(self, chromid):
#         return self._causal_markers[chromid]
#
#     def set_causal_markers(self, chromid, cm):
#         self._causal_markers[chromid] = asarray(cm, int)
#
#     def get_marker_pos(self, chromid):
#         return self._marker_pos[chromid]
#
#     def set_marker_pos(self, chromid, mp):
#         self._marker_pos[chromid] = asarray(mp, int)
#
#     @property
#     def methods(self):
#         return self._pv.keys()
#
#     def pv(self, method, chromid):
#         return self._pv[method][chromid]
#
#     def null_lml(self, method):
#         return self._null_lml[method]
#
#     def elapsed(self, method):
#         return self._elapsed[method]
#
#     def error_status(self, method):
#         return self._error_status[method]
#
#     def error_msg(self, method):
#         return self._error_msg[method]
#
#     def set_y(self, y):
#         y = asarray(y, float)
#         assert y.ndim == 1
#         self._y = y
#
#     def get_y(self):
#         return self._y
#
#     def set_ntrials(self, ntrials):
#         ntrials = asarray(ntrials, int)
#         assert ntrials.ndim == 1
#         self._ntrials = ntrials
#
#     def get_ntrials(self):
#         return self._ntrials
#
#     def set_suc(self, suc):
#         suc = asarray(suc, int)
#         assert suc.ndim == 1
#         self._suc = suc
#
#     def get_suc(self):
#         return self._suc
#
#     def set_pv(self, method, chromid, pv):
#         pv = asarray(pv, float)
#         assert pv.ndim == 1
#         if method not in self._pv:
#             self._pv[method] = dict()
#         self._pv[method][chromid] = pv
#
#     def set_null_lml(self, method, null_lml):
#         self._null_lml[method] = float(null_lml)
#
#     def set_beta_snp(self, method, chromid, beta_snp):
#         beta_snp = asarray(beta_snp, float)
#         assert beta_snp.ndim == 1
#         if method not in self._beta_snp:
#             self._beta_snp[method] = dict()
#         self._beta_snp[method] = beta_snp
#
#     def get_beta_snp(self, method, chromid):
#         return self._beta_snp[method][chromid]
#
#     def set_error_status(self, method, error_status):
#         self._error_status[method] = int(error_status)
#
#     def set_error_msg(self, method, error_msg):
#         self._error_msg[method] = str(error_msg)
#
#     def set_elapsed(self, method, elapsed):
#         self._elapsed[method] = float(elapsed)
#
#     def __str__(self):
#         from humanfriendly import format_timespan
#         from tabulate import tabulate
#
#         msg = ''
#         cm = lu.str_.array2string(self.get_causal_markers('chrom02'))
#         table = [['Causal markers', cm]]
#         mp = lu.str_.array2string(self.get_marker_pos())
#         table.append(['Marker positions', mp])
#         msg += tabulate(table) + '\n'
#
#
#         for method in self.methods:
#             table = []
#             table.append(['Method', method])
#             table.append(['Elapsed', format_timespan(self.elapsed(method))])
#             table.append(['LML0', str(self.null_lml(method))])
#             pv = asarray(self.pv(method, 'chrom02'))
#             table.append(['Pvals', lu.str_.array2string(pv)])
#             table.append(['Error status', str(self.error_status(method))])
#             table.append(['Error message', str(self.error_msg(method))])
#             msg += tabulate(table) + '\n'
#
#         return msg
#
#
# class GwasTaskResult(TaskResult):
#     __slots__  = ['_pv', '_null_lml', '_beta_snp',
#                   '_elapsed', '_error_status', '_error_msg',
#                   '_marker_pos']
#
#     """docstring for GwasTaskResult"""
#     def __init__(self, task_id):
#         super(GwasTaskResult, self).__init__(task_id)
#
#         self._pv = dict()
#         self._null_lml = dict()
#         self._beta_snp = dict()
#         self._elapsed = dict()
#         self._error_status = dict()
#         self._error_msg = dict()
#         self._marker_pos = None
#
#     # def fill(self, grp):
#     #     methods = grp.attrs['methods']
#     #     if 'marker_pos' in grp:
#     #         self._marker_pos = grp['marker_pos'].value
#     #     for m in methods:
#     #         for (k, v) in grp[m].iteritems():
#     #             getattr(self, '_' + k)[m] = v.value
#     #         for (k, v) in grp[m].attrs.iteritems():
#     #             getattr(self, '_' + k)[m] = v
#     #     TaskResult.fill(self, grp)
#
#     def get_marker_pos(self):
#         return self._marker_pos
#
#     def set_marker_pos(self, mp):
#         self._marker_pos = asarray(mp, int)
#
#     @property
#     def methods(self):
#         return self._pv.keys()
#
#     def pv(self, method):
#         return self._pv[method]
#
#     def null_lml(self, method):
#         return self._null_lml[method]
#
#     def elapsed(self, method):
#         return self._elapsed[method]
#
#     def error_status(self, method):
#         return self._error_status[method]
#
#     def error_msg(self, method):
#         return self._error_msg[method]
#
#     def set_pv(self, method, pv):
#         pv = asarray(pv, float)
#         assert pv.ndim == 1
#         self._pv[method] = pv
#
#     def set_null_lml(self, method, null_lml):
#         self._null_lml[method] = float(null_lml)
#
#     def set_beta_snp(self, method, beta_snp):
#         beta_snp = asarray(beta_snp, float)
#         assert beta_snp.ndim == 1
#         self._beta_snp[method] = beta_snp
#
#     def set_error_status(self, method, error_status):
#         self._error_status[method] = int(error_status)
#
#     def set_error_msg(self, method, error_msg):
#         self._error_msg[method] = str(error_msg)
#
#     def set_elapsed(self, method, elapsed):
#         self._elapsed[method] = float(elapsed)
#
#     # def store(self, grp):
#     #     grp.attrs.create('methods', asarray(self.methods, nstr))
#     #     for m in self.methods:
#     #         g = grp.create_group(m)
#     #         g.create_dataset('pv', data=self._pv[m])
#     #         g.attrs.create('null_lml', self._null_lml[m])
#     #         g.attrs.create('elapsed', self._elapsed[m])
#     #         g.attrs.create('error_status', self._error_status[m])
#     #         g.attrs.create('error_msg', nstr(self._error_msg[m]))
#     #     grp.attrs.create('total_elapsed', self.total_elapsed)
#     #     grp.attrs.create('task_id', self.task_id)
#     #     grp.attrs.create('task_result_cls', nstr(self.__class__.__name__))
#     #
#     #     if self._marker_pos is not None:
#     #         grp.create_dataset('marker_pos', data=self._marker_pos)
#
def load_tasks(fpath, verbose=True):
    with BeginEnd('Loading tasks', silent=not verbose):
        if os.path.exists(fpath):
            if verbose:
                print "Exist %s" % fpath
        else:
            print "Does not exist %s" % fpath
        tasks = lu.pickle_.unpickle(fpath)
        if verbose:
            print('   %d tasks found  ' % len(tasks))
    return tasks

def store_tasks(tasks, fpath):
    if os.path.exists(fpath):
        return
    lu.pickle_.pickle({t.task_id:t for t in tasks}, fpath)
    # print('   %d tasks stored   ' % len(tasks))

def load_task_args(fpath):
    return lu.pickle_.unpickle(fpath)

def store_task_args(task_args, fpath):
    if os.path.exists(fpath):
        return
    lu.pickle_.pickle(task_args, fpath)

def collect_task_results(folder, force_cache=False):
    if force_cache:
        fpath = os.path.join(folder, 'all.pkl')
    else:
        with BeginEnd('Merging task results'):
            fpath = lu.pickle_.pickle_merge(folder)
    if fpath is None:
        return fpath
    with BeginEnd('Unpickling merged task results'):
        out = lu.pickle_.unpickle(fpath)
    return out

def store_task_results(task_results, fpath):
    with BeginEnd('Storing task results'):
        lu.pickle_.pickle({tr.task_id:tr for tr in task_results}, fpath)
        print('   %d task results stored   ' % len(task_results))

def tasks_summary(tasks):
    import workspace
    from collections import OrderedDict

    if len(tasks) == 0:
        return ''

    wid = tasks[0]._workspace_id
    eid = tasks[0]._experiment_id

    e = workspace.get_experiment(wid, eid)

    args = e.get_task_args().get_names()

    args.sort()
    d = OrderedDict([(k, set()) for k in args])

    for task in tasks:
        for a in args:
            d[a].add(getattr(task, a))

    for a in args:
        d[a] = list(d[a])
        d[a].sort(key= lambda x: float(x) if lu.isfloat(x) else x)

    table = zip(d.keys(), [lu.str_.summarize(v) for v in d.values()])
    return '*** Task summary ***\n' + tabulate(table)
