==============
Tornado-MySQL
==============

.. image:: https://travis-ci.org/PyMySQL/Tornado-MySQL.svg?branch=tornado
   :target: https://travis-ci.org/PyMySQL/Tornado-MySQL

.. contents::

This package contains a fork of PyMySQL supporting Tornado.

Example
-------

Simple
~~~~~~~

.. include:: example.py
    :code: python

pool
~~~~~

.. include:: example_pool.py
    :code: python

Requirements
-------------

* Python -- one of the following:

  - CPython_ 2.7 or >= 3.3
  - PyPy_ >= 2.3.1

* MySQL Server -- one of the following:

  - MySQL_ >= 4.1
  - MariaDB_ >= 5.1

.. _CPython: http://www.python.org/
.. _PyPy: http://pypy.org/
.. _MySQL: http://www.mysql.com/
.. _MariaDB: https://mariadb.org/


Installation
------------

The last stable release is available on PyPI and can be installed with ``pip``::

    $ pip install Tornado-MySQL

Alternatively (e.g. if ``pip`` is not available), a tarball can be downloaded
from GitHub and installed with Setuptools::

    $ # X.X is the desired PyMySQL version (e.g. 0.5 or 0.6).
    $ curl -L https://github.com/PyMySQL/PyMySQL/tarball/pymysql-X.X | tar xz
    $ cd PyMySQL*
    $ python setup.py install
    $ # The folder PyMySQL* can be safely removed now.

Test Suite
----------

If you would like to run the test suite, first copy the file
``.travis.databases.json`` to ``pymysql/tests/databases.json`` and edit the new
file to match your MySQL configuration::

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
