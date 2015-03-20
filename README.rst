=======
PyMySQL
=======

.. image:: https://travis-ci.org/PyMySQL/PyMySQL.svg?branch=master
   :target: https://travis-ci.org/PyMySQL/PyMySQL

.. contents::

This package contains a pure-Python MySQL client library. The goal of PyMySQL
is to be a drop-in replacement for MySQLdb and work on CPython, PyPy and IronPython.


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


CRUD Exapmle
------------

The following examples make use of a simple table

.. code:: sql

   CREATE TABLE `users` (
       `id` int(11) NOT NULL,
       `email` varchar(255) COLLATE utf8_bin NOT NULL,
       `password` varchar(255) COLLATE utf8_bin NOT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
   AUTO_INCREMENT=1 ;


Create new record
~~~~~~~~~~~~~~~~~

.. code:: python

    import pymysql
    connection = pymysql.connect(host='host',
                                 user='user',
                                 passwd='passwd',
                                 db='db')
    with closing(connection.cursor()) as cursor:
        sql = ("INSERT INTO `users` "
               "(`email`, `password`) "
               "VALUES ('webmaster@python.org', 'very-secret');")
        cursor.execute(sql)
    connection.close()


Read records
~~~~~~~~~~~~

.. code:: python

    import pymysql
    connection = pymysql.connect(host='host',
                                 user='user',
                                 passwd='passwd',
                                 db='db',
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()

    sql = ("SELECT `id`, `password` "
           "FROM `users` WHERE `email`=%s") % email

    try:
        cursor.execute(sql)
        result = cursor.fetchone()
    finally:
        connection.close()
    return result['id']


Update records
~~~~~~~~~~~~~~

.. code:: python

    import pymysql
    connection = pymysql.connect(host='host',
                                 user='user',
                                 passwd='passwd',
                                 db='db')
    cursor = connection.cursor()

    sql = ("UPDATE `users` SET `email`= 'maxmustermann@email.de' "
           "WHERE `id` = %i LIMIT 1") % 42

    try:
        cursor.execute(sql)
        connection.commit()
    finally:
        connection.close()


Delete records
~~~~~~~~~~~~~~

.. code:: python

    import pymysql
    connection = pymysql.connect(host='host',
                                 user='user',
                                 passwd='passwd',
                                 db='db')
    cursor = connection.cursor()

    sql = ("DELETE FROM `users` WHERE `id` = %i") % 42

    try:
        cursor.execute(sql)
        connection.commit()
    finally:
        connection.close()


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
