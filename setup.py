# -*- encoding: utf-8 -*-

import io
import re
from os.path import dirname
from os.path import join

from setuptools import find_packages, setup


def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ).read()


project_name = 'spark-architecture-demo'
module_name = project_name
regex_found_badges = re.compile('^.. start-badges.*^.. end-badges', re.M | re.S)
dependencies = [
    'pyspark==3.2.2',
    'pyarrow==0.11.1'
]

setup(
    name=module_name,
    version='0.1.0-SNAPSHOT',
    author='Damavis',
    author_email='info@damavis.com',
    long_description='Demo architecture',
    url='www.damavis.com',
    python_requires='>=3.7',
    test_suite='nose.collector',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages('src', exclude=['tests', 'tests.*']),
    package_dir={'': 'src'},
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Operating System :: Unix',
    ],
    tests_require=["nose", "spark-testing-base", "mock_repository"] + dependencies,
    install_requires=dependencies
)
