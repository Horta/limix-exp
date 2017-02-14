import os

try:
    import configparser
except ImportError:
    import ConfigParser as configparser


def _conf():
    home = os.path.expanduser('~')
    fp = os.path.join(home, '.config', 'exp', 'config')
    cp = configparser.ConfigParser()
    cp.read(fp)
    return cp


conf = _conf()
