from __future__ import absolute_import

import imp
import inspect
import re


def _fetch_entity(script_filepath, regex, entity, def_within=True):
    mod_name = inspect.getmodulename(script_filepath)
    mod = imp.load_source(mod_name, script_filepath)

    funcs = []
    for func in inspect.getmembers(mod, entity):
        if def_within:
            if mod_name != func[1].__module__:
                continue
        func_name = func[0]
        m = re.match(regex, func_name)
        if m:
            funcs.append(func[1])

    return funcs


def fetch_functions(script_filepath, regex):
    """Fetches functions from a file.

    :param str script_filepath: file path.
    :param str regex: regular expression for matching function names.
    :returns: a list of functions.
    """
    return _fetch_entity(script_filepath, regex, inspect.isfunction)


def fetch_classes(script_filepath, regex):
    """Fetches classes from a file.

    :param str script_filepath: file path.
    :param str regex: regular expression for matching class names.
    :returns: a list of classes.
    """
    return _fetch_entity(script_filepath, regex, inspect.isclass)
