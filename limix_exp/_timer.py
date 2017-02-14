from __future__ import absolute_import

import time
import humanfriendly as hf

class Timer(object):
    """Prints the elapsed time after the execution of a block of code
       finishes.
    """
    def __init__(self, verbose=True):
        self._verbose = verbose
        self._tstart = None
        self.elapsed = None

    def __enter__(self):
        self._tstart = time.time()
        return self

    def __exit__(self, type_, value_, traceback_):
        self.elapsed = time.time() - self._tstart
        if self._verbose:
            print('Elapsed time: %s.' % hf.format_timespan(self.elapsed))
