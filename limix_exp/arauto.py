from __future__ import division, absolute_import
import os
from argparse import ArgumentParser
from . import config
from . import task
from .workspace import get_workspace
from .workspace import get_experiment
from limix_misc.inspect_ import fetch_functions

def _fetch_filter_file(script_filepath):
    funcs = fetch_functions(script_filepath, r'task_filter')
    if len(funcs) > 0:
        return funcs[0]
    return None

def _fetch_filter(script_filepath):
    if os.path.exists(script_filepath):
        return _fetch_filter_file(script_filepath)
    return eval("lambda task: " + script_filepath)

def do_root(_1, _2):
    print(config.root_dir())

def do_see(args, rargs):
    from limix_plot.show import show

    w = get_workspace(args.workspace_id)
    e = w.get_experiment(args.experiment_id)
    tasks = [task for task in e.get_tasks() if task.finished]

    if args.task_filter is not None:
        filter_ = _fetch_filter(args.task_filter)
        if filter_ is not None:
            tasks = [t for t in tasks if filter_(t)]

    if len(tasks) == 0:
        print('No finished task has been found.')
        return

    if args.group_by is not None:
        group_by = args.group_by.split(',')
    else:
        group_by = None

    properties = w.get_properties()
    plot_cls = w.get_plot_class(args.plot_class_name)
    p = plot_cls(args.workspace_id, args.experiment_id, properties, tasks,
                 rargs)
    p.group_by = group_by
    p.plot()
    show()

def do_job_info(args, _):
    e = get_experiment(args.workspace_id, args.experiment_id)
    job = e.get_job(args.job)
    print job
    if job.submitted:
        if args.stdout:
            print '--- STDOUT BEGIN ---'
            print job.get_bjob().stdout()
            print '--- STDOUT END ---'
        if args.stderr:
            print '--- STDERR BEGIN ---'
            print job.get_bjob().stderr()
            print '--- STDERR END ---'
        if args.result:
            tasks = job.get_tasks()
            for task in tasks:
                print task.get_result()

def do_run_job(args, _):
    if args.debug:
        import ipdb; ipdb.set_trace()
    e = get_experiment(args.workspace_id, args.experiment_id)
    e.run_job(args.job, args.progress, args.dryrun, force=args.force)

def do_rm_exp(args, _):
    w = get_workspace(args.workspace_id)
    w.rm_experiment(args.experiment_id)

def do_submit_jobs(args, _):
    e = get_experiment(args.workspace_id, args.experiment_id)
    requests = args.requests
    if requests is not None:
        requests = requests.split(',')
    e.submit_jobs(args.dryrun, requests=requests)

def do_work_info(args, _):
    w = get_workspace(args.workspace_id)
    print w

def do_exp_info(args, _):
    e = get_experiment(args.workspace_id, args.experiment_id)
    print e
    if args.tasks:
        tasks = e.get_tasks()
        print task.tasks_summary(tasks)
    if args.finished_jobs:
        jobs = [j for j in e.get_jobs() if j.finished]
        jobids = sorted([j.jobid for j in jobs])
        print 'Finished job IDs: %s' % str(jobids)

def entry_point():
    p = ArgumentParser()
    sub = p.add_subparsers()

    s = sub.add_parser('root')
    s.set_defaults(func=do_root)

    s = sub.add_parser('work-info')
    s.add_argument('workspace_id')
    s.set_defaults(func=do_work_info)

    s = sub.add_parser('exp-info')
    s.add_argument('workspace_id')
    s.add_argument('experiment_id')
    s.add_argument('--tasks', dest='tasks', action='store_true')
    s.add_argument('--no_tasks', dest='tasks', action='store_false')
    s.add_argument('--finished_jobs', dest='finished_jobs', action='store_true')
    s.add_argument('--no_finished_jobs', dest='finished_jobs', action='store_false')
    s.set_defaults(func=do_exp_info, task_args=False, finished_jobs=False)

    s = sub.add_parser('rm-exp')
    s.add_argument('workspace_id')
    s.add_argument('experiment_id')
    s.set_defaults(func=do_rm_exp)

    s = sub.add_parser('run-job')
    s.add_argument('workspace_id')
    s.add_argument('experiment_id')
    s.add_argument('--debug', dest='debug', action='store_true')
    s.add_argument('--no-debug', dest='debug', action='store_false')
    s.add_argument('--dryrun', dest='dryrun', action='store_true')
    s.add_argument('--no-dryrun', dest='dryrun', action='store_false')
    s.add_argument('--progress', dest='progress', action='store_true')
    s.add_argument('--no-progress', dest='progress', action='store_false')
    s.add_argument('--force', dest='force', action='store_true')
    s.add_argument('--no-force', dest='force', action='store_false')
    s.add_argument('job', type=int)
    s.set_defaults(func=do_run_job, dryrun=False, progress=True, force=False,
                   debug=False)

    s = sub.add_parser('submit-jobs')
    s.add_argument('workspace_id')
    s.add_argument('experiment_id')
    s.add_argument('--requests', default=None)
    s.add_argument('--dryrun', dest='dryrun', action='store_true')
    s.add_argument('--no-dryrun', dest='dryrun', action='store_false')
    s.set_defaults(func=do_submit_jobs, dryrun=False)

    s = sub.add_parser('job-info')
    s.add_argument('workspace_id')
    s.add_argument('experiment_id')
    s.add_argument('--result', dest='result', action='store_true')
    s.add_argument('--no-result', dest='result', action='store_false')
    s.add_argument('--stdout', dest='stdout', action='store_true')
    s.add_argument('--no-stdout', dest='stdout', action='store_false')
    s.add_argument('--stderr', dest='stderr', action='store_true')
    s.add_argument('--no-stderr', dest='stderr', action='store_false')
    s.add_argument('job', type=int)
    s.set_defaults(func=do_job_info, result=False, stdout=True, stderr=True)

    s = sub.add_parser('see')
    s.add_argument('workspace_id')
    s.add_argument('experiment_id')
    s.add_argument('plot_class_name')
    s.add_argument('--group_by', default=None)
    s.add_argument('--task_filter', default=None)
    s.add_argument('--grid', dest='grid', action='store_true')
    s.add_argument('--no-grid', dest='grid', action='store_false')
    s.set_defaults(func=do_see, grid=True)

    args, rargs = p.parse_known_args()
    func = args.func
    del args.func
    func(args, rargs)
