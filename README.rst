====================
PyMySQL Installation
====================

.. contents::
..
  
This package contains a pure-Python MySQL client library.
Documentation on the MySQL client/server protocol can be found here:
http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol
If you would like to run the test suite, edit the config parameters in
pymysql/tests/base.py. The goal of pymysql is to be a drop-in
replacement for MySQLdb and work on CPython, Jython, IronPython and PyPy.
We test for compatibility by simply changing the import statements
in the Django MySQL backend and running its unit tests as well
as running it against the MySQLdb and myconnpy unit tests.

Requirements
-------------

+ Python 2.6, 2.7 or 3.3

 * http://www.python.org/
 
* MySQL 4.1 or higher
    
 * protocol41 support, experimental 4.0 support

Installation
------------

# pip install PyMySQL  
# ... or ...  
# python setup.py install

