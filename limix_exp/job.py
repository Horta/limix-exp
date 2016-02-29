import numpy as np
from limix_lsf import clusterrun
from limix_misc import pickle_
from limix_misc.report import BeginEnd, ProgressBar
from limix_misc.time_ import Timer
from hcache import Cached, cached

class Job(Cached):
    def __init__(self, workspace_id, experiment_id, jobid):
        super(Job, self).__init__()
        self.jobid = int(jobid)
        self._workspace_id = workspace_id
        self._experiment_id = experiment_id
        self.finished = False
        self.submitted = False
        self.bjobid = None
        self.brunid = None

    @property
    def failed(self):
        if self.submitted:
            bjob = self.get_bjob()
            stats = bjob.exit_status()
            return (stats is not None and stats != 0)
        return False


    @cached
    def get_bjob(self):
        bjob = clusterrun.get_bjob(self.brunid, self.bjobid)
        return bjob

    @property
    def task_ids(self):
        import workspace
        e = workspace.get_experiment(self._workspace_id, self._experiment_id)

        task2job = np.floor(e.njobs * np.arange(e.ntasks) / e.ntasks)
        task2job = np.asarray(task2job, int)
        task_ids = list(np.where(task2job == self.jobid)[0])

        return task_ids

    def get_tasks(self):
        task_ids = self.task_ids

        import workspace
        e = workspace.get_experiment(self._workspace_id, self._experiment_id)

        tasks = e.get_tasks()
        return [tasks[tid] for tid in task_ids]

    def run(self, progress):
        tasks = self.get_tasks()
        task_results = []

        if progress:
            p = ProgressBar(len(tasks))

        for (i, task) in enumerate(tasks):
            with Timer() as timer:
                tr = task.run()
            tr.total_elapsed = timer.elapsed
            if progress:
                p.update(i+1)
            task_results.append(tr)

        if progress:
            p.finish()

        self.finished = True

        return task_results

    def __str__(self):
        from tabulate import tabulate
        table = [['Job ID', str(self.jobid)]]
        table.append(['Submitted', str(self.submitted)])
        table.append(['Finished', str(self.finished)])
        table.append(['# tasks', len(self.task_ids)])
        table.append(['Task IDs', str(self.task_ids)])

        bjob_status = 'N/A'
        bjob_exit_status = 'N/A'
        bjob_os_id = 'N/A'

        if self.submitted:
            bjob_status = self.get_bjob().stat()
            bjob_exit_status = str(self.get_bjob().exit_status())
            bjob_os_id = self.get_bjob().os_jobid

        table.append(['Bjob status', bjob_status])
        table.append(['Bjob exit status', bjob_exit_status])
        table.append(['Bjob OS id', bjob_os_id])

        return tabulate(table)

def store_jobs(jobs, fpath):
    with BeginEnd('Storing jobs'):
        pickle_.pickle({t.jobid:t for t in jobs}, fpath)
        print('   %d jobs stored   ' % len(jobs))

def store_job(job, fpath):
    pickle_.pickle(job, fpath)

def load_job(fpath):
    return pickle_.unpickle(fpath)

def collect_jobs(folder):
    with BeginEnd('Merging jobs'):
        fpath = pickle_.pickle_merge(folder)
    if fpath is None:
        return None
    with BeginEnd('Unpickling merged jobs'):
        out = pickle_.unpickle(fpath)
    return out
