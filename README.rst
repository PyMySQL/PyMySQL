=======
PyMySQL
=======

.. image:: https://travis-ci.org/PyMySQL/PyMySQL.svg?branch=master
   :target: https://travis-ci.org/PyMySQL/PyMySQL

.. image:: https://landscape.io/github/PyMySQL/PyMySQL/master/landscape.svg?style=flat
   :target: https://landscape.io/github/PyMySQL/PyMySQL/master
   :alt: Code Health

.. contents::

This package contains a pure-Python MySQL client library. The goal of PyMySQL
is to be a drop-in replacement for MySQLdb and work on CPython, PyPy,
IronPython and Jython.


Requirements
-------------

* Python -- one of the following:

  - CPython_ >= 2.6 or >= 3.3
  - PyPy_ >= 2.3
  - IronPython_ 2.7

* MySQL Server -- one of the following:

  - MySQL_ >= 4.1
  - MariaDB_ >= 5.1

.. _CPython: http://www.python.org/
.. _PyPy: http://pypy.org/
.. _IronPython: http://ironpython.net/
.. _MySQL: http://www.mysql.com/
.. _MariaDB: https://mariadb.org/


Installation
------------

The last stable release is available on PyPI and can be installed with ``pip``::

    $ pip install PyMySQL

Alternatively (e.g. if ``pip`` is not available), a tarball can be downloaded
from GitHub and installed with Setuptools::

    $ # X.X is the desired PyMySQL version (e.g. 0.5 or 0.6).
    $ curl -L https://github.com/PyMySQL/PyMySQL/tarball/pymysql-X.X | tar xz
    $ cd PyMySQL*
    $ python setup.py install
    $ # The folder PyMySQL* can be safely removed now.

Test Suite
----------

If you would like to run the test suite, create database for test like this::

    mysql -e 'create database test_pymysql  DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'
    mysql -e 'create database test_pymysql2 DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'

Then, copy the file ``.travis.databases.json`` to ``pymysql/tests/databases.json``
and edit the new file to match your MySQL configuration::

    $ cp .travis.databases.json pymysql/tests/databases.json
    $ $EDITOR pymysql/tests/databases.json

To run all the tests, execute the script ``runtests.py``::

    $ python runtests.py

A ``tox.ini`` file is also provided for conveniently running tests on multiple
Python versions::

    $ tox

Resources
---------

DB-API 2.0: http://www.python.org/dev/peps/pep-0249

MySQL Reference Manuals: http://dev.mysql.com/doc/

MySQL client/server protocol:
http://dev.mysql.com/doc/internals/en/client-server-protocol.html

PyMySQL mailing list: https://groups.google.com/forum/#!forum/pymysql-users

License
-------

PyMySQL is released under the MIT License. See LICENSE for more information.
