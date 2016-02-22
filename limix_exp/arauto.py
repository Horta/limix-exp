from argparse import ArgumentParser
import config
from limix_exp import get_experiment
from limix_exp import task

def do_root(_):
    print(config.root_dir())

def do_exp_info(args):
    import ipdb; ipdb.set_trace()

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

    s = sub.add_parser('exp-info')
    s.add_argument('workspace_id')
    s.add_argument('experiment_id')
    s.add_argument('--tasks', dest='tasks', action='store_true')
    s.add_argument('--no_tasks', dest='tasks', action='store_false')
    s.add_argument('--finished_jobs', dest='finished_jobs', action='store_true')
    s.add_argument('--no_finished_jobs', dest='finished_jobs', action='store_false')
    s.set_defaults(func=do_exp_info, task_args=False, finished_jobs=False)

    args = p.parse_args()
    func = args.func
    del args.func
    func(args)
