import os
import ConfigParser

def _conf():
    home = os.path.expanduser('~')
    fp = os.path.join(home, '.config', 'exp', 'config')
    cp = ConfigParser.ConfigParser()
    cp.read(fp)
    return cp

conf = _conf()
