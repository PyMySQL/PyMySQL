#!/usr/bin/env python
import io
from setuptools import setup, find_packages

version = "0.9.3"

with io.open('./README.rst', encoding='utf-8') as f:
    readme = f.read()

setup(
    name="PyMySQL",
    version=version,
    url='https://github.com/PyMySQL/PyMySQL/',
    project_urls={
        "Documentation": "https://pymysql.readthedocs.io/",
    },
    description='Pure Python MySQL Driver',
    long_description=readme,
    packages=find_packages(exclude=['tests*', 'pymysql.tests*']),
    extras_require={
        "rsa": ["cryptography"],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    keywords="MySQL",
)
