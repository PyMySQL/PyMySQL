#!/usr/bin/env python
from setuptools import setup, find_packages

version_tuple = __import__('pymysql').VERSION

if version_tuple[3] is not None:
    version = "%d.%d.%d_%s" % version_tuple
else:
    version = "%d.%d.%d" % version_tuple[:3]

setup(
    name="PyMySQL",
    version=version,
    url='https://github.com/PyMySQL/PyMySQL/',
    author='yutaka.matsubara',
    author_email='yutaka.matsubara@gmail.com',
    maintainer='INADA Naoki',
    maintainer_email='songofacandy@gmail.com',
    description='Pure Python MySQL Driver',
    license="MIT",
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
)
