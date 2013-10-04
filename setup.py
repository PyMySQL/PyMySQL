#!/usr/bin/env python
from setuptools import setup, find_packages

version_tuple = __import__('pymysql').VERSION

if version_tuple[2] is not None:
    version = "%d.%d_%s" % version_tuple
else:
    version = "%d.%d" % version_tuple[:2]

try:
    with open('README.rst') as f:
        readme = f.read()
except IOError:
    readme = ''

setup(
    name="PyMySQL",
    version=version,
    url='https://github.com/PyMySQL/PyMySQL/',
    download_url = 'https://github.com/PyMySQL/PyMySQL/tarball/pymysql-%s' % version,
    author='yutaka.matsubara',
    author_email='yutaka.matsubara@gmail.com',
    maintainer='Marcel Rodrigues',
    maintainer_email='marcelgmr@gmail.com',
    description='Pure-Python MySQL Driver',
    long_description=readme,
    license="MIT",
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ]
)
