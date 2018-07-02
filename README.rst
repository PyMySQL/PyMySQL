.. image:: https://readthedocs.org/projects/pymysql/badge/?version=latest
    :target: https://pymysql.readthedocs.io/
    :alt: Documentation Status

.. image:: https://badge.fury.io/py/PyMySQL.svg
    :target: https://badge.fury.io/py/PyMySQL

.. image:: https://travis-ci.org/PyMySQL/PyMySQL.svg?branch=master
    :target: https://travis-ci.org/PyMySQL/PyMySQL

.. image:: https://coveralls.io/repos/PyMySQL/PyMySQL/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/PyMySQL/PyMySQL?branch=master

.. image:: https://img.shields.io/badge/license-MIT-blue.svg
    :target: https://github.com/PyMySQL/PyMySQL/blob/master/LICENSE


PyMySQL
=======

.. contents:: Table of Contents
   :local:

This package contains a pure-Python MySQL client library, based on `PEP 249`_.

Most public APIs are compatible with mysqlclient and MySQLdb.

NOTE: PyMySQL doesn't support low level APIs `_mysql` provides like `data_seek`,
`store_result`, and `use_result`. You should use high level APIs defined in `PEP 249`_.
But some APIs like `autocommit` and `ping` are supported because `PEP 249`_ doesn't cover
their usecase.

.. _`PEP 249`: https://www.python.org/dev/peps/pep-0249/


Requirements
-------------

* Python -- one of the following:

  - CPython_ : 2.7 and >= 3.4
  - PyPy_ : Latest version

* MySQL Server -- one of the following:

  - MySQL_ >= 5.5
  - MariaDB_ >= 5.5

.. _CPython: https://www.python.org/
.. _PyPy: https://pypy.org/
.. _MySQL: https://www.mysql.com/
.. _MariaDB: https://mariadb.org/


Installation
------------

Package is uploaded on `PyPI <https://pypi.org/project/PyMySQL>`_.

You can install it with pip::

    $ pip3 install PyMySQL


Documentation
-------------

Documentation is available online: https://pymysql.readthedocs.io/

For support, please refer to the `StackOverflow
<https://stackoverflow.com/questions/tagged/pymysql>`_.

Example
-------

The following examples make use of a simple table

.. code:: sql

   CREATE TABLE `users` (
       `id` int(11) NOT NULL AUTO_INCREMENT,
       `email` varchar(255) COLLATE utf8_bin NOT NULL,
       `password` varchar(255) COLLATE utf8_bin NOT NULL,
       PRIMARY KEY (`id`)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
   AUTO_INCREMENT=1 ;


.. code:: python

    import pymysql.cursors

    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='user',
                                 password='passwd',
                                 db='db',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
            cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
            cursor.execute(sql, ('webmaster@python.org',))
            result = cursor.fetchone()
            print(result)
    finally:
        connection.close()

This example will print:

.. code:: python

    {'password': 'very-secret', 'id': 1}


Resources
---------

* DB-API 2.0: http://www.python.org/dev/peps/pep-0249

* MySQL Reference Manuals: http://dev.mysql.com/doc/

* MySQL client/server protocol:
  http://dev.mysql.com/doc/internals/en/client-server-protocol.html

* "Connector" channel in MySQL Community Slack:
  http://lefred.be/mysql-community-on-slack/

* PyMySQL mailing list: https://groups.google.com/forum/#!forum/pymysql-users

License
-------

PyMySQL is released under the MIT License. See LICENSE for more information.
