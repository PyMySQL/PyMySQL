#!/usr/bin/env python
from setuptools import setup, find_packages

version_tuple = __import__('tornado_mysql').VERSION

if version_tuple[3] is not None:
    version = "%d.%d.%d_%s" % version_tuple
else:
    version = "%d.%d.%d" % version_tuple[:3]

try:
    with open('README.rst') as f:
        readme = f.read()
except IOError:
    readme = ''

setup(
    name="PyMySQL",
    version=version,
    url='https://github.com/PyMySQL/Tornado-MySQL',
    author='yutaka.matsubara',
    author_email='yutaka.matsubara@gmail.com',
    maintainer='INADA Naoki',
    maintainer_email='songofacandy@gmail.com',
    description='Pure-Python MySQL Driver for Tornado',
    install_requires=['tornado>=4.0'],
    long_description=readme,
    license="MIT",
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ]
)
