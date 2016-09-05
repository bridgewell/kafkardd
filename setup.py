# -*- coding: utf-8 -*-
import uuid

from pip.req import parse_requirements
from setuptools import setup, find_packages

import kafkardd

def requirements(path):
    return [str(r.req) for r in parse_requirements(path, session=uuid.uuid1())]


setup(
    name='kafkardd',
    version=kafkardd.__version__,
    author=kafkardd.__author__,
    author_email=kafkardd.__email__,
    description='Fetch Kafka messages to Spark RDD object for processing',
    long_description=__doc__,
    license=kafkardd.__license__,
    packages=find_packages(),
    install_requires=requirements('requirements.txt'),
    dependency_links=['git+http://git@192.168.65.220:10080/rd/bwproto.git@master#subdirectory=python'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)
