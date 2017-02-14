from __future__ import absolute_import

import collections
from os import listdir, walk
from os.path import basename, isdir, join

from pickle_blosc import pickle, unpickle
from tqdm import tqdm

from ._path import cp, folder_hash, make_sure_path_exists, temp_folder

def pickle_merge(folder):
    """Merges pickle files and save it to `all.pkl`."""
    file_list = _get_file_list(folder)

    if len(file_list) == 0:
        print('There is nothing to merge because no file' +
              ' has been found in %s.' % folder)
        return

    ha = folder_hash(folder, ['all.pkl', '.folder_hash'])

    subfolders = [d for d in listdir(folder) if isdir(join(folder, d))]

    with temp_folder() as tf:
        for sf in subfolders:
            make_sure_path_exists(join(tf, sf))
            cp(join(folder, sf), join(tf, sf))
        file_list = _get_file_list(tf)

        out = _merge(file_list)

    print('Storing pickles...')
    pickle(out, join(folder, 'all.pkl'))
    _save_cache(folder, ha)

    return out


def _save_cache(folder, lastmodif_hash):
    fpath = join(folder, '.folder_hash')
    with open(fpath, 'w') as f:
        f.write(lastmodif_hash)


def _get_file_list(folder):
    file_list = []
    for (dir_, _, files) in walk(folder):
        if dir_ == folder:
            continue
        for f in files:
            fpath = join(dir_, f)
            if fpath.endswith('pkl') and basename(fpath) != 'all.pkl':
                file_list.append(fpath)
    return file_list


def _merge(file_list):
    out = dict()
    for fpath in tqdm(file_list, desc='Merging files'):
        d = unpickle(fpath)
        if isinstance(d, collections.Iterable):
            out.update(d)
        else:
            key = basename(fpath).split('.')[0]
            out[int(key)] = d
    return out
