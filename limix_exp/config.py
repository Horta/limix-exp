import os

def conf():
    home = os.path.expanduser('~')
    f = open(os.path.join(home, '.config', 'exp', 'config'))
    conf_ = dict()
    for line in f.readline():
        part = line.split('=')
        if len(part) != 2:
            continue
        name = part[0]
        value = part[1]
        conf_[name] = value
    return conf_
