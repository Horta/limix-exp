from __future__ import print_function

import contextlib
import errno
import hashlib
import shutil
import subprocess
from sys import stderr
import tempfile
from distutils.spawn import find_executable
from os import makedirs, utime
from os.path import basename


@contextlib.contextmanager
def temp_folder():
    folder = tempfile.mkdtemp()
    try:
        yield folder
    finally:
        shutil.rmtree(folder)


def make_sure_path_exists(path):
    """Creates a path recursively if necessary.
    """
    try:
        makedirs(path)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise


def cp(folder_src, folder_dst):
    """Uses cp shell command to copy files."""
    retcode = subprocess.call(
        "cp " + folder_src + "/* " + folder_dst, shell=True)

    if retcode < 0:
        print("Child was terminated by signal %d" % retcode, file=stderr)


def touch(fname, times=None):
    """Creates an empty file."""
    with open(fname, 'a'):
        utime(fname, times)


def folder_hash(folder, exclude_files=None):
    """Recursively compute the hash of all files in a folder and sum it up.
    """
    if exclude_files is None:
        exclude_files = []

    if not _bin_exists('md5deep'):
        raise EnvironmentError("Couldn't not find md5deep.")

    print("folder_hash...")
    out = subprocess.check_output('md5deep -r %s' % folder, shell=True)
    lines = sorted(out.strip(b'\n').split(b'\n'))

    m = hashlib.md5()
    for line in lines:
        hash_ = line[0:32]
        fp = line[34:]
        if basename(fp) not in exclude_files:
            m.update(hash_)
    return m.hexdigest()


def _bin_exists(name):
    """Checks whether an executable file exists."""
    return find_executable(name) is not None
