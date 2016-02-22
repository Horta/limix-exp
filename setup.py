import os
if os.path.exists('MANIFEST'): os.remove('MANIFEST')
import sys
import imp
import textwrap

try:
    imp.find_module('numpy')
except ImportError:
    print('Fatal: could not import numpy. Please, make sure it is installed.')
    sys.exit(1)

MAJOR               = 0
MINOR               = 1
MICRO               = 0
ISRELEASED          = False
VERSION             = '%d.%d.%d' % (MAJOR, MINOR, MICRO)

if sys.version_info[0] >= 3:
    import builtins
else:
    import __builtin__ as builtins

# This is a bit hackish: we are setting a global variable so that the main
# limix-exp __init__ can detect if it is being loaded by the setup routine, to
# avoid attempting to load components that aren't built yet.  While ugly, it's
# a lot more robust than what was previously being used.
builtins.__LIMIX_EXP_SETUP__ = True

def parse_setuppy_commands():
    """Check the commands and respond appropriately.  Disable broken commands.
    Return a boolean value for whether or not to run the build or not (avoid
    parsing Cython and template files if False).
    """
    if len(sys.argv) < 2:
        # User forgot to give an argument probably, let setuptools handle that.
        return True

    info_commands = ['--help-commands', '--name', '--version', '-V',
         '--fullname', '--author', '--author-email',
         '--maintainer', '--maintainer-email', '--contact',
         '--contact-email', '--url', '--license', '--description',
         '--long-description', '--platforms', '--classifiers',
         '--keywords', '--provides', '--requires', '--obsoletes']
    # Add commands that do more than print info, but also don't need Cython and
    # template parsing.
    info_commands.extend(['egg_info', 'install_egg_info', 'rotate'])

    for command in info_commands:
        if command in sys.argv[1:]:
            return False

    good_commands = ('develop', 'sdist', 'build', 'build_ext', 'build_py',
                 'build_clib', 'build_scripts', 'bdist_wheel', 'bdist_rpm',
                 'bdist_wininst', 'bdist_msi', 'bdist_mpkg')

    for command in good_commands:
        if command in sys.argv[1:]:
            return True

    # The following commands are supported, but we need to show more
    # useful messages to the user
    if 'install' in sys.argv[1:]:
        print(textwrap.dedent("""
            Note: if you need reliable uninstall behavior, then install
            with pip instead of using `setup.py install`:
              - `pip install .`       (from a git repo or downloaded source
                                       release)
              - `pip install limix-exp`   (last limix-exp release on PyPi)
            """))
        return True

    if '--help' in sys.argv[1:] or '-h' in sys.argv[1]:
        print(textwrap.dedent("""
        limix-exp-specific help
        -------------------
        To install limix-exp from here with reliable uninstall, we recommend
        that you use `pip install .`. To install the latest limix-exp release
        from PyPi, use `pip install limix-exp`.
        Setuptools commands help
        ------------------------
        """))
        return False

    return True

def get_test_suite():
    from unittest import TestLoader
    return TestLoader().discover('limix_exp')

def configuration(parent_package='', top_path=None):
    from numpy.distutils.misc_util import Configuration

    config = Configuration(None, parent_package, top_path)
    config.set_options(ignore_setup_xxx_py=True,
                       assume_default_configuration=True,
                       delegate_options_to_subpackages=True,
                       quiet=True)

    config.add_subpackage('limix_exp')

    config.get_version('limix_exp/version.py') # sets config.version

    return config

def setup_package():
    path = os.path.realpath(__file__)
    dirname = os.path.dirname(path)
    mod = imp.load_source('__init__',
                          os.path.join(dirname, 'build_util', '__init__.py'))
    write_version_py = mod.write_version_py
    get_version_info = mod.get_version_info
    generate_cython = mod.generate_cython

    src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    old_path = os.getcwd()
    os.chdir(src_path)
    sys.path.insert(0, src_path)

    # Rewrite the version file everytime
    filename = os.path.join(dirname, 'limix_exp', 'version.py')
    write_version_py(VERSION, ISRELEASED, filename='limix_exp/version.py')

    build_requires = ['limix_util']

    metadata = dict(
        name='limix-exp',
        maintainer = "Limix Developers",
        maintainer_email = "horta@ebi.ac.uk",
        test_suite='setup.get_test_suite',
        packages=['limix_exp'],
        install_requires=build_requires,
        setup_requires=build_requires
    )

    run_build = parse_setuppy_commands()

    from setuptools import setup

    if run_build:
        from numpy.distutils.core import setup
        metadata['configuration'] = configuration
    else:
        if len(sys.argv) >= 2 and sys.argv[1] == 'bdist_wheel':
            # bdist_wheel needs setuptools
            import setuptools
        from numpy.distutils.core import setup

        cwd = os.path.abspath(os.path.dirname(__file__))
        if not os.path.exists(os.path.join(cwd, 'PKG-INFO')):
            # Generate Cython sources, unless building from source release
            generate_cython()

        # Version number is added to metadata inside configuration() if build
        # is run.
        metadata['version'] = get_version_info(VERSION, ISRELEASED,
                                               filename=filename)[0]

    try:
        setup(**metadata)
    finally:
        del sys.path[0]
        os.chdir(old_path)

if __name__ == '__main__':
    setup_package()
