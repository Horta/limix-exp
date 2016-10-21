
import os
from os.path import join
import random
import logging
from humanfriendly import format_size
from humanfriendly import parse_size
from tabulate import tabulate
from limix_lsf import clusterrun
from limix_lsf.clusterrun import ClusterRun
from limix_util import path as path_
from limix_util.path import make_sure_path_exists
from limix_util.report import BeginEnd, ProgressBar
from hcache import Cached, cached
from . import task
from . import job
from .config import conf

class Experiment(Cached):
    def __init__(self, workspace_id, experiment_id, properties):
        super(Experiment, self).__init__()
        self._workspace_id = workspace_id
        self._experiment_id = experiment_id

        self.script_filepath = None
        self._task_id_counter = -1
        self.njobs = None
        self.mkl_nthreads = 1
        self.nprocs = 1
        self._job_megabytes = None
        self._properties = properties
        self.auto_run_done = False
        self.finish_setup_done = False
        self._logger = logging.getLogger(__name__)
        self._logger.debug('Experiment %s has been created.', experiment_id)

    @property
    def runid(self):
        fp = join(self.folder, '.runid')
        if os.path.exists(fp):
            return open(fp, 'r').read()

    @runid.setter
    def runid(self, v):
        fp = join(self.folder, '.runid')
        open(fp, 'w').write(v)

    def kill_bjobs(self):
        jobs = self.get_jobs()
        runids = set([j.brunid for j in jobs if j.submitted])
        for ri in runids:
            if clusterrun.exists(ri):
                clusterrun.load(ri).kill()
                clusterrun.rm(ri)
            else:
                self._logger.warn('Cluster run %s does not exist.' % ri)

    @property
    def job_memory(self):
        nbytes = int(round(self._job_megabytes * 1024. * 1024.))
        return format_size(nbytes)

    @job_memory.setter
    def job_memory(self, siz):
        nbytes = parse_size(siz)
        self._job_megabytes = int(round(nbytes / 1024. / 1024.))

    def exists(self):
        folder = self.folder
        return os.path.exists(folder)

    def create_task(self):
        self._task_id_counter += 1
        return task.Task(self._workspace_id, self._experiment_id,
                         self._task_id_counter)

    def get_task(self, task_id):
        return self._get_tasks()[task_id]

    @cached
    def _get_task_results(self):
        fpath = join(self.folder, 'result')
        return task.collect_task_results(fpath, force_cache=False)

    @cached
    def get_task_results(self):
        return list(self._get_task_results().values())

    def get_task_result(self, task_id):
        return self._get_task_results().get(task_id)

    def has_task_result(self, task_id):
        return int(task_id) in self._get_task_results()

    @property
    def folder(self):
        return join(conf.get('default', 'base_dir'),
                    self._workspace_id,
                    self._experiment_id)

    @property
    def figures_folder(self):
        f = join(self.folder, 'figs')
        make_sure_path_exists(f)
        return f

    def split_folder(self, jobid):
        return str(int(jobid/1000))

    @cached
    def _get_tasks(self):
        fpath = join(self.folder, 'tasks.pkl')
        return task.load_tasks(fpath)

    def get_tasks(self):
        return list(self._get_tasks().values())

    @property
    def ntasks(self):
        return len(self.get_tasks())

    def get_task_args(self):
        fpath = join(self.folder, 'task_args.pkl')
        return task.load_task_args(fpath)

    def define_task_args(self, task_args):
        raise NotImplementedError

    def generate_tasks(self):
        raise NotImplementedError

    def generate_jobs(self, workspace_id):
        wid = workspace_id
        eid = self._experiment_id
        jobs = [job.Job(wid, eid, i) for i in range(self.njobs)]
        return jobs

    def do_task(self, task):
        raise NotImplementedError

    @property
    def tasks_setup_done(self):
        fpath = join(self.folder, 'tasks.pkl')
        return os.path.exists(fpath)

    def _store_tasks(self, tasks):
        fpath = join(self.folder, 'tasks.pkl')
        task.store_tasks(tasks, fpath)

    def _store_jobs(self, jobs):
        with BeginEnd('Storing jobs'):
            pb = ProgressBar(len(jobs))
            for (i, j) in enumerate(jobs):
                fp = self.job_path(j.jobid)
                make_sure_path_exists(os.path.dirname(fp))
                job.store_job(j, fp)
                pb.update(i+1)
            pb.finish()

    def job_path(self, jobid):
        fp = join(self.folder, 'job', self.split_folder(jobid))
        fp = join(fp, str(jobid) + '.pkl')
        return fp

    def task_result_path(self, jobid):
        fp = join(self.folder, 'result', self.split_folder(jobid))
        fp = join(fp, str(jobid) + '.pkl')
        return fp

    def _store_task_args(self, task_args):
        fpath = join(self.folder, 'task_args.pkl')
        task.store_task_args(task_args, fpath)

    def run_job(self, jobid, progress=True, dryrun=False, force=False):
        job_ = job.load_job(self.job_path(jobid))

        if job_.finished and not force:
            print("Job %d has already finished." % jobid)
            return

        task_results = job_.run(progress)

        if not dryrun:
            job.store_job(job_, self.job_path(job_.jobid))
            fp = self.task_result_path(job_.jobid)
            make_sure_path_exists(os.path.dirname(fp))
            task.store_task_results(task_results, fp)

    @property
    def are_init_jobs_files_generated(self):
        fp = join(self.folder, '.init_jobs_files_generated')
        return os.path.exists(fp)

    def finish_setup(self):
        make_sure_path_exists(self.folder)

        ta = task.TaskArgs()
        self.define_task_args(ta)
        self._store_task_args(ta)

        if self.tasks_setup_done:
            tasks = self.get_tasks()
        else:
            with BeginEnd('Generating tasks'):
                tasks = self.generate_tasks()
            self._store_tasks(tasks)

        ntasks = len(tasks)
        if self.njobs is None:
            self.njobs = ntasks
        else:
            self.njobs = min(self.njobs, ntasks)

        if not self.are_init_jobs_files_generated:
            with BeginEnd('Generating jobs'):
                jobs = self.generate_jobs(self._workspace_id)
                if len(jobs) == 0:
                    raise Exception('No job has been generated.')
                print('   %d generated jobs   ' % len(jobs))
            self._store_jobs(jobs)
            fp = join(self.folder, '.init_jobs_files_generated')
            path_.touch(fp)

        self.finish_setup_done = True

    @cached
    def get_job(self, jobid):
        return job.load_job(self.job_path(jobid))

    @cached
    def get_jobs(self):
        folder = join(self.folder, 'job')
        jobs = job.collect_jobs(folder)
        keys = list(jobs.keys())
        vals = list(jobs.values())
        return [j for (_, j) in sorted(zip(keys, vals))]

    def _get_job_task_ids(self, jobid):
        import numpy as np
        ntasks = len(self.get_tasks())
        task2job = np.floor(self.njobs * np.arange(ntasks) / ntasks)
        task2job = np.asarray(task2job, int)
        task_ids = list(np.where(task2job == jobid)[0])
        return task_ids

    def resubmit(self, jobid):
        job = self.get_job(jobid)
        cr = clusterrun.load(self.runid)
        cr.resubmit(job.bjobid)

    def submit_jobs(self, dryrun, requests=None, queue=None, verbose=False):
        jobs = self.get_jobs()
        myrand = random.Random(937628)
        myrand.shuffle(jobs)

        title = '/%s/%s' % (self._workspace_id, self._experiment_id)
        cmd = ClusterRun(title)
        cmd.queue = queue
        cmd.memory = self.job_memory
        cmd.mkl_nthreads = self.mkl_nthreads
        cmd.nprocs = self.nprocs
        if requests is not None:
            for request in requests:
                cmd.request(request)

        for j in jobs:
            a = ['arauto']
            if self._logger.isEnabledFor(logging.DEBUG):
                a += ['--verbose']
            a += ['rjob', self._workspace_id]
            a += [self._experiment_id]
            a += [j.jobid]
            if dryrun:
                a += ['--dryrun']
            else:
                a += ['--no-dryrun']
            a += ['--no-progress']
            cmd.add(a)

        self.runid = cmd.run(dryrun=dryrun)
        for (i, j) in enumerate(cmd.jobs):
            jobs[i].bjobid = j.jobid
            jobs[i].brunid = j.runid
            jobs[i].submitted = True

        if not dryrun:
            self._store_jobs(jobs)
            cmd.store()

    def method_errors(self):
        tasks = self.get_tasks()
        if len(tasks) == 0:
            return
        task_results = self.get_task_results()
        properties = self._properties

        methods = task_results[0].methods
        err_msgs = {m:set() for m in methods}
        for tr in task_results:
            for method in methods:
                s = err_msgs[method]
                if tr.error_status != 0:
                    s.add(tr.error_msg(method))

        ml = {m:properties[m]['label'] for m in methods}

        for m in methods:
            print('Error messages for %s:' % ml[m])
            if len(err_msgs[m]) > 0:
                for msg in err_msgs[m]:
                    print(msg)

    def _store_task_results(self, folder_split, jobid, task_results):
        base = join(self.folder, 'result', folder_split)
        make_sure_path_exists(base)
        fpath = join(base, '%d.pkl' % int(jobid))
        task.store_task_results(task_results, fpath)

    @property
    def bgroup(self):
        return '/' + self._workspace_id + '/' + self._experiment_id

    def __str__(self):
        with BeginEnd('Compiling info table'):
            table = []
            table.append(['# jobs', str(self.njobs)])
            table.append(['# tasks', str(self.ntasks)])

            bjobs_finished = [j.get_bjob() for j in self.get_jobs() if j.submitted and j.finished]

            nsub = sum([1 for j in self.get_jobs() if j.submitted])
            table.append(['# submitted jobs', str(nsub)])

            npend = sum([1 for j in self.get_jobs() if (j.submitted and
                                                        j.get_bjob().ispending()
                                                       )])
            table.append(['# pending jobs', str(npend)])

            nrun = sum([1 for j in self.get_jobs() if (j.submitted and
                                                       j.get_bjob().isrunning()
                                                      )])
            table.append(['# running jobs', str(nrun)])

            nfin = sum([1 for j in self.get_jobs() if j.finished])
            table.append(['# finished jobs', str(nfin)])

            failed_jobids = [j.jobid for j in self.get_jobs() if j.failed]
            nfail = len(failed_jobids)
            table.append(['# failed jobs', str(nfail)])

            resource_info = [bj.resource_info() for bj in bjobs_finished]

            max_memories = [r['max_memory'] for r in resource_info if r is not None and r['max_memory'] is not None]
            if len(max_memories) > 0:
                max_memory = max(max_memories)
            else:
                max_memory = None
            req_memories = [r['req_memory'] for r in resource_info if r is not None and r['req_memory'] is not None]
            if len(req_memories) > 0:
                req_memory = max(req_memories)
            else:
                req_memory = None
            table.append(['max used memory', format_size(max_memory) if max_memory is not None else 'n/a'])
            table.append(['max req. memory', format_size(req_memory) if req_memory is not None else 'n/a'])

            msg = tabulate(table)

            if nfail > 0:
                msg += '\nFailed jobs: ' + str(failed_jobids)
                msg += '\n'

        return msg


def _namespace2task(workspace_id, experiment_id, n):
    n = vars(n)
    t = task.Task(workspace_id, experiment_id, n['task_id'])
    for (k, v) in list(n.items()):
        setattr(t, k, v)
    return t
