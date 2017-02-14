from __future__ import unicode_literals

from sys import stdout
from time import time
from ._string import make_sure_unicode

class BeginEnd(object):
    """Prints message before and after the end of block of code."""
    def __init__(self, task, silent=False):
        self._task = make_sure_unicode(task)
        self._start = None
        self._silent = silent

    def __enter__(self):
        self._start = time()
        if not self._silent:
            stdout.write('%s... ' % self._task)
            stdout.flush()

    def __exit__(self, *args):
        elapsed = time() - self._start
        if not self._silent:
            stdout.write('%.2f s.\n' % elapsed)
            stdout.flush()
