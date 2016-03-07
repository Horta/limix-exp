from argparse import ArgumentParser
import limix_tool as lu
import gwarped_exp as gwe
from gwarped_exp.plot.cal import CalPlot
from slugify import slugify
import os

def _task_hash(tasks):
    import hashlib
    taskids = sorted([t.task_id for t in tasks])
    m = hashlib.md5()
    m.update(str(taskids))
    return m.hexdigest()

def namespace2filename(ns):
    ns = str(ns)
    ns = ns.replace('namespace', '').replace(',', '_')
    return slugify(unicode(ns))

def _sort_key(k):
    if lu.isfloat(k):
        return float(k)
    return k

def see(tasks, group_by=None, grid=True, folder=None, fprefix='see_cal'):
    if (group_by is None) or group_by is not None and grid:
        hp = CalPlot(tasks, group_by)
        fname = fprefix + '.1.png'
        dst_fp = os.path.join(folder, fname)

        i = 1
        while os.path.exists(dst_fp):
            dst_fp = dst_fp.replace('.%d.png' % i, '.%d.png' % (i+1))
            i += 1

        with lu.BeginEnd('Plotting calibration results'):
            hp.plot()

        with lu.BeginEnd("Saving figure to %s" % dst_fp):
            lu.plot_.show(dst_fp=dst_fp)

    elif not grid and group_by is not None:
        tasks = lu.group_by(tasks, group_by, sort_key=_sort_key)
        def _callback(d, opt):
            n = len(d)

            if opt is None:
                return
            plot_this = n > 0 and len(opt['names']) == opt['level']

            if plot_this:
                hp = CalPlot(d)
                fname = fprefix
                for i in xrange(len(opt['names'])):
                    fname += '_' + opt['names'][i] + '_' + str(opt['values'][i])
                fname += '.png'
                dst_fp = os.path.join(folder, fname)
                with lu.BeginEnd('Plotting calibration results'):
                    hp.plot()

                with lu.BeginEnd("Saving figure to %s" % dst_fp):
                    lu.plot_.show(dst_fp=dst_fp)
            else:
                opts = []
                for i in xrange(len(d)):

                    nopt = dict(names=opt['names'], level=opt['level']+1,
                                values=opt['values']+[d.keys()[i]])
                    opts.append(nopt)
                return opts

        opts = dict(names=group_by, level=0, values=[])
        lu.traverse_dict(tasks, _callback, opts)
