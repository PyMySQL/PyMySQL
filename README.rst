====================
CyMySQL Installation
====================

What's CyMySQL
--------------

This package contains a python MySQL client library.

It is a fork project from PyMySQL http://www.pymysql.org/ .

PyMySQL is written by Yutaka Matsubara <yutaka.matsubara@gmail.com>
as pure python database driver. CyMySQL is powered by Cython, it still can work
without Cython as a pure python driver.

Documentation on the MySQL client/server protocol can be found here:
http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol .

If you would like to run the test suite, edit the config parameters in
pymysql/tests/base.py. The goal of cymysql is to be a drop-in replacement
for MySQLdb and work on CPython 2.6+.

Requirements
-------------

- Python 2.6 or higher
- Cython 17.1 or higher
- MySQL 4.1 or higher
    
Installation
------------

# python setup.py install

