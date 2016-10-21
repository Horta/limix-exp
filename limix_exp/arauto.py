
import os
from argparse import ArgumentParser
from .config import conf
from . import task
from . import workspace
from limix_util.inspect import fetch_functions
import logging

def _fetch_filter_file(script_filepath):
    funcs = fetch_functions(script_filepath, r'task_filter')
    if len(funcs) > 0:
        return funcs[0]
    return None

def _fetch_filter(fp_or_code):
    if os.path.exists(fp_or_code):
        return _fetch_filter_file(fp_or_code)
    return eval("lambda task: " + fp_or_code)

def do_root():
    print(conf.get('default', 'base_dir'))

def do_see(args, rargs):

    w = workspace.get_workspace(args.workspace_id)
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

    import matplotlib.pyplot as plt
    plt.style.use('nice')
    if args.figsize is None:
        fig = plt.figure()
    else:
        figsize = (float(s) for s in args.figsize.split(','))
        fig = plt.figure(figsize=figsize)

    properties = w.get_properties()
    plot_cls = w.get_plot_class(args.plot_class_name)
    p = plot_cls(args.workspace_id, args.experiment_id, properties, tasks,
                 rargs, fig=fig)
    p.group_by = group_by
    p.grid = args.grid
    p.plot()

    if args.outfile is None:
        from limix_plot.show import show
        show(fig)
    else:
        from limix_plot.savefig import savefig
        savefig(args.outfile, fig)

def do_jinfo(args):
    e = workspace.get_experiment(args.workspace_id, args.experiment_id)
    job = e.get_job(args.job)
    print(job)
    if job.submitted:
        if args.stdout:
            print('--- STDOUT BEGIN ---')
            print(job.get_bjob().stdout())
            print('--- STDOUT END ---')
        if args.stderr:
            print('--- STDERR BEGIN ---')
            print(job.get_bjob().stderr())
            print('--- STDERR END ---')
        if args.result:
            tasks = job.get_tasks()
            for task in tasks:
                print(task.get_result())

def do_rjob(args):
    if args.debug:
        import ipdb; ipdb.set_trace()
    e = workspace.get_experiment(args.workspace_id, args.experiment_id)
    e.run_job(args.job, args.progress, args.dryrun, force=args.force)

def do_rm_exp(args):
    w = workspace.get_workspace(args.workspace_id)
    w.rm_experiment(args.experiment_id)

def do_err(args):
    e = workspace.get_experiment(args.workspace_id, args.experiment_id)
    e.method_errors()

def do_sjobs(args):
    e = workspace.get_experiment(args.workspace_id, args.experiment_id)
    requests = args.requests
    if requests is not None:
        requests = requests.split(',')
    e.submit_jobs(args.dryrun, requests=requests, queue=args.queue)

def do_winfo(args):
    if workspace.exists(args.workspace_id):
        w = workspace.get_workspace(args.workspace_id)
        print(w)
    else:
        print('Workspace %s does not exist.' % args.workspace_id)

def do_einfo(args):
    e = workspace.get_experiment(args.workspace_id, args.experiment_id)
    print(e)
    if args.tasks:
        tasks = e.get_tasks()
        print(task.tasks_summary(tasks))
    if args.finished_jobs:
        jobs = [j for j in e.get_jobs() if j.finished]
        jobids = sorted([j.jobid for j in jobs])
        print('Finished job IDs: %s' % str(jobids))

def parse_einfo(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')
    p.add_argument('experiment_id')
    p.add_argument('--tasks', dest='tasks', action='store_true')
    p.add_argument('--no_tasks', dest='tasks', action='store_false')
    p.add_argument('--finished_jobs', dest='finished_jobs', action='store_true')
    p.add_argument('--no_finished_jobs', dest='finished_jobs', action='store_false')
    p.set_defaults(task_args=False, finished_jobs=False)

    args = p.parse_args(args)
    do_einfo(args)

def parse_jinfo(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')
    p.add_argument('experiment_id')
    p.add_argument('job', type=int)
    p.add_argument('--result', dest='result', action='store_true')
    p.add_argument('--no-result', dest='result', action='store_false')
    p.add_argument('--stdout', dest='stdout', action='store_true')
    p.add_argument('--no-stdout', dest='stdout', action='store_false')
    p.add_argument('--stderr', dest='stderr', action='store_true')
    p.add_argument('--no-stderr', dest='stderr', action='store_false')
    p.set_defaults(result=False, stdout=True, stderr=True)

    args = p.parse_args(args)
    do_jinfo(args)

def parse_rm_exp(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')
    p.add_argument('experiment_id')
    p.set_defaults(resubmit=False)

    args = p.parse_args(args)
    do_rm_exp(args)

def parse_sjobs(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')
    p.add_argument('experiment_id')
    p.add_argument('--queue', default=None)
    p.add_argument('--requests', default=None)
    p.add_argument('--dryrun', dest='dryrun', action='store_true')
    p.add_argument('--no-dryrun', dest='dryrun', action='store_false')
    p.set_defaults(dryrun=False)

    args = p.parse_args(args)
    do_sjobs(args)

def parse_rjob(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')
    p.add_argument('experiment_id')
    p.add_argument('job', type=int)
    p.add_argument('--debug', dest='debug', action='store_true')
    p.add_argument('--no-debug', dest='debug', action='store_false')
    p.add_argument('--dryrun', dest='dryrun', action='store_true')
    p.add_argument('--no-dryrun', dest='dryrun', action='store_false')
    p.add_argument('--progress', dest='progress', action='store_true')
    p.add_argument('--no-progress', dest='progress', action='store_false')
    p.add_argument('--force', dest='force', action='store_true')
    p.add_argument('--no-force', dest='force', action='store_false')
    p.set_defaults(dryrun=False, progress=True, force=False, debug=False)

    args = p.parse_args(args)
    do_rjob(args)

def parse_see(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')
    p.add_argument('experiment_id')
    p.add_argument('plot_class_name')
    p.add_argument('--outfile', '-o', dest='outfile', default=None)
    p.add_argument('--style', default='nice')
    p.add_argument('--figsize', default=None)
    p.add_argument('--group_by', default=None)
    p.add_argument('--task_filter', default=None)
    p.add_argument('--grid', dest='grid', action='store_true')
    p.add_argument('--no-grid', dest='grid', action='store_false')
    p.set_defaults(grid=True)

    args, rargs = p.parse_known_args(args)
    do_see(args, rargs)

def parse_winfo(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')

    args = p.parse_args(args)
    do_winfo(args)

def parse_err(args):
    p = ArgumentParser()
    p.add_argument('workspace_id')
    p.add_argument('experiment_id')

    args = p.parse_args(args)
    do_err(args)

def entry_point():
    p = ArgumentParser()
    p.add_argument("-v", "--verbose", help="increase output verbosity",
                   action="store_true")

    sub = p.add_subparsers()

    s = sub.add_parser('root')
    s.set_defaults(func=lambda _: do_root())

    s = sub.add_parser('winfo')
    s.add_argument('workspace_id')
    s.set_defaults(func=parse_winfo)

    s = sub.add_parser('rm-exp')
    s.set_defaults(func=parse_rm_exp)

    s = sub.add_parser('einfo')
    s.set_defaults(func=parse_einfo)

    s = sub.add_parser('rjob')
    s.set_defaults(func=parse_rjob)

    s = sub.add_parser('sjobs')
    s.set_defaults(func=parse_sjobs)

    s = sub.add_parser('err')
    s.set_defaults(func=parse_err)

    s = sub.add_parser('jinfo')
    s.set_defaults(func=parse_jinfo)

    s = sub.add_parser('see')
    s.set_defaults(func=parse_see)

    args, rargs = p.parse_known_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    func = args.func
    del args.func
    func(rargs)
