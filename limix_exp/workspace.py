
from argparse import ArgumentParser
import re
import inspect
import os
from os.path import join
import json
import imp
import shutil
from .config import conf
from . import experiment
from limix_util.report import BeginEnd
from limix_util.inspect import fetch_classes, fetch_functions
import limix_lsf
import logging

_workspaces = dict()
def get_workspace(workspace_id):
    if workspace_id not in _workspaces:
        _workspaces[workspace_id] = Workspace(workspace_id)
    return _workspaces[workspace_id]

def get_experiment(workspace_id, experiment_id):
    w = get_workspace(workspace_id)
    if w is None:
        raise Exception('There is no workspace called %s.' % workspace_id)

    e = w.get_experiment(experiment_id)
    if e is None:
        raise Exception('There is no experiment '
                        'called %s in the workspace %s.'
                        % (experiment_id, workspace_id))

    return e

def exists(workspace_id):
    folder = join(conf.get('default', 'base_dir'), workspace_id)
    return os.path.exists(folder)

def get_workspace_ids():
    files = os.listdir(conf.get('default', 'base_dir'))
    return [f for f in files if os.path.isdir(f)]

class Workspace(object):
    def __init__(self, workspace_id):
        super(Workspace, self).__init__()
        self._workspace_id = workspace_id
        self._script_filepath = None
        self._experiments = dict()
        self.force_cache = False
        self._logger = logging.getLogger(__name__)
        self._logger.debug('Workspace %s has been created.', workspace_id)

    def rm_experiment(self, experiment_id):
        e = self.get_experiment(experiment_id)
        e.kill_bjobs()
        if os.path.exists(e.folder):
            with BeginEnd('Removing folder'):
                shutil.rmtree(e.folder)

    def get_properties(self):
        try:
            with open(join(self.folder, 'properties.json')) as json_file:
                return json.load(json_file)
        except ValueError as e:
            print(e)
            print("File: %s", join(self.folder, 'properties.json'))
            print('File content:')
            print(open(join(self.folder, 'properties.json')).read())
            os.system("hostname")

    def get_plot_class(self, name):
        return self._get_plot_classes_map()[name]

    def _get_plot_classes_map(self):
        f = join(self.folder, 'plot.json')
        self._logger.info("Reading plot config file %s.", f)
        with open(f) as json_file:
            fpaths = json.load(json_file)
            fpaths = [str(fpath) for fpath in fpaths]

        plot_classes_map = dict()
        for fp in fpaths:
            self._logger.info("Reading file %s.", fp)
            clss = fetch_classes(fp, '.*Plot')
            for clsi in clss:
                plot_classes_map[clsi.__name__] = clsi
        return plot_classes_map

    @property
    def folder(self):
        return join(conf.get('default', 'base_dir'), self._workspace_id)

    def _get_auto_run(self, experiment_id):
        auto_runs_map = self._get_auto_runs_map()
        # if experiment_id not in auto_runs_map:
            # return None
        return auto_runs_map[experiment_id]

    def get_experiment(self, experiment_id):
        if experiment_id not in self._experiments:
            self._setup_experiment(experiment_id)
        return self._experiments[experiment_id]

    def _setup_experiment(self, experiment_id):
        self._experiments[experiment_id] =\
            experiment.Experiment(self._workspace_id, experiment_id,
                                  self.get_properties())
        auto_run = self._get_auto_run(experiment_id)
        if auto_run is None:
            return
        auto_run(self._experiments[experiment_id])
        self._experiments[experiment_id].auto_run_done = True
        self._experiments[experiment_id].finish_setup()

    def _get_auto_runs_map(self):
        fps = self._auto_run_filepaths()
        auto_runs_map = dict()
        for fp in fps:
            self._logger.info("Reading file %s.", fp)
            funcs = fetch_functions(fp, r'^auto_run_.+$')
            auto_runs_map.update([(f.__name__[9:], f) for f in funcs])
        return auto_runs_map

    @property
    def dataset_folder(self):
        return os.path.join(self.folder, 'dataset')

    def _auto_run_filepaths(self):
        f = join(self.folder, 'auto_run.json')
        if not os.path.exists(f):
            self._logger.warn("File %s does not exist.", f)
            return []
        with open(f) as json_file:
            fpaths = json.load(json_file)
            fpaths = [str(fpath) for fpath in fpaths]
            return fpaths

    def auto_run(self, args):
        filepaths = self._auto_run_filepaths()
        for fp in filepaths:
            self._auto_run(fp, args)

    def _auto_run(self, script_filepath, args):

        p = ArgumentParser()
        p.add_argument('--dryrun', dest='dryrun', action='store_true')
        p.add_argument('--no-dryrun', dest='dryrun', action='store_false')
        p.add_argument('--queue', default=None)
        p.set_defaults(dryrun=False, parallel=True)

        args = p.parse_args(args)

        lista = self._get_generate_tasks(script_filepath)
        for (experiment_id, func) in lista:
            exp = self.get_experiment(experiment_id)
            func(exp)

            try:
                exp.submit_jobs(args.dryrun, queue=args.queue)
            except KeyboardInterrupt:
                shutil.rmtree(exp.folder)
                raise

            if args.dryrun:
                shutil.rmtree(exp.folder)

    def remove(self, experiment_id, jobs_too=False):
        if experiment_id is None:
            print('Nothing to remove.')
            return
        if len(experiment_id) == 0:
            print('Nothing to remove.')
            return
        with BeginEnd('Removing folder'):
            shutil.rmtree(join(self.folder, experiment_id))

        if jobs_too:
            bgroup = '/%s/%s' % (self._workspace_id, experiment_id)
            if exists(bgroup):
                limix_lsf.util.kill_group(bgroup, True)

    def _call_job(self, args):
        experiment_id = args.experiment_id
        (experiment_id, auto_run_func) = self._get_generate_tasks(experiment_id)
        exp = self.get_experiment(experiment_id)
        exp.script_filepath = self._script_filepath
        auto_run_func(exp)
        exp.start(check_existence=False)

    def _get_generate_tasks(self, script_filepath, experiment_id=None):
        script_name = os.path.basename(script_filepath)
        mod_name = os.path.splitext(script_name)[0]
        mod = imp.load_source(mod_name, script_filepath)

        lista = []
        for func in inspect.getmembers(mod, inspect.isfunction):
            func_name = func[0]
            m = re.match(r'^auto_run_(.+)$', func_name)
            if m:
                exp_name = m.group(1)
                if experiment_id is None:
                    lista.append((exp_name, func[1]))
                elif exp_name == experiment_id:
                    return exp_name, func[1]

        return lista

    def __str__(self):
        from tabulate import tabulate

        files = os.listdir(self.folder)
        exp_ids = [f for f in files if (os.path.isdir(join(self.folder, f)) and
                                        join(self.folder, f,
                                             'generate_tasks.txt'))]

        auto_run_err = set()
        tasks_setup_err = set()
        finish_setup_err = set()
        imp_err = set()
        suc = set()

        for ei in exp_ids:
            try:
                e = self.get_experiment(ei)
                if not e.auto_run_done:
                    auto_run_err.add(ei)
                elif not e.tasks_setup_done:
                    tasks_setup_err.add(ei)
                elif not e.finish_setup_done:
                    finish_setup_err.add(ei)
                else:
                    suc.add(ei)
            except ImportError:
                imp_err.add(ei)

        table = []
        auto_run_fps = '\n'.join(self._auto_run_filepaths())
        table.append(['auto_run files', auto_run_fps])
        table.append(['# experiments', str(len(exp_ids))])


        table.append(["# auto_run err", len(auto_run_err)])
        table.append(["# tasks setup err", len(tasks_setup_err)])
        table.append(["# finish setup err", len(finish_setup_err)])
        table.append(["# import err", len(imp_err)])
        table.append(["# successful", len(suc)])

        msg = tabulate(table)

        return msg

def _get_auto_runs_map(script_filepath):
    script_name = os.path.basename(script_filepath)
    mod_name = os.path.splitext(script_name)[0]
    mod = imp.load_source(mod_name, script_filepath)

    auto_run_map = dict()
    for func in inspect.getmembers(mod, inspect.isfunction):
        func_name = func[0]
        m = re.match(r'^auto_run_(.+)$', func_name)
        if m:
            exp_name = m.group(1)
            auto_run_map.update({exp_name:func[1]})

    return auto_run_map
