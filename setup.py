import os
import sys

from setuptools import find_packages, setup


def setup_package():
    src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    old_path = os.getcwd()
    os.chdir(src_path)
    sys.path.insert(0, src_path)

    needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
    pytest_runner = ['pytest-runner'] if needs_pytest else []

    setup_requires = [] + pytest_runner
    install_requires = [
        'pytest', 'scipy>=0.17', 'numpy>=1.9', 'tabulate', 'humanfriendly',
        'pickle-mixin', 'pickle-blosc', 'limix-lsf', 'joblib'
    ]
    tests_require = ['pytest']

    metadata = dict(
        name='limix-exp',
        version='1.0.5.dev0',
        maintainer="Danilo Horta",
        maintainer_email="horta@ebi.ac.uk",
        license="MIT",
        url='http://github.com/Horta/limix-exp',
        packages=find_packages(),
        zip_safe=True,
        install_requires=install_requires,
        setup_requires=setup_requires,
        tests_require=tests_require,
        include_package_data=True,
        entry_points={
            'console_scripts': [
                'arauto = limix_exp.arauto:entry_point',
                'iarauto = limix_exp.iarauto:entry_point'
            ]
        })

    try:
        setup(**metadata)
    finally:
        del sys.path[0]
        os.chdir(old_path)


if __name__ == '__main__':
    setup_package()
