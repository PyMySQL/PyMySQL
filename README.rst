.. image:: https://readthedocs.org/projects/pymysql/badge/?version=latest
    :target: https://pymysql.readthedocs.io/
    :alt: Documentation Status

.. image:: https://coveralls.io/repos/PyMySQL/PyMySQL/badge.svg?branch=main&service=github
    :target: https://coveralls.io/github/PyMySQL/PyMySQL?branch=main

.. image:: https://img.shields.io/lgtm/grade/python/g/PyMySQL/PyMySQL.svg?logo=lgtm&logoWidth=18
    :target: https://lgtm.com/projects/g/PyMySQL/PyMySQL/context:python


PyMySQL
=======

.. contents:: Table of Contents
   :local:

This package contains a pure-Python MySQL client library, based on `PEP 249`_.

.. _`PEP 249`: https://www.python.org/dev/peps/pep-0249/


Requirements
-------------

* Python -- one of the following:

  - CPython_ : 3.7 and newer
  - PyPy_ : Latest 3.x version

* MySQL Server -- one of the following:

  - MySQL_ >= 5.7
  - MariaDB_ >= 10.3

.. _CPython: https://www.python.org/
.. _PyPy: https://pypy.org/
.. _MySQL: https://www.mysql.com/
.. _MariaDB: https://mariadb.org/


Installation
------------

Package is uploaded on `PyPI <https://pypi.org/project/PyMySQL>`_.

You can install it with pip::

    $ python3 -m pip install PyMySQL

To use "sha256_password" or "caching_sha2_password" for authenticate,
you need to install additional dependency::

   $ python3 -m pip install PyMySQL[rsa]

To use MariaDB's "ed25519" authentication method, you need to install
additional dependency::

   $ python3 -m pip install PyMySQL[ed25519]


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
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   AUTO_INCREMENT=1 ;


.. code:: python

    import pymysql.cursors

    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='user',
                                 password='passwd',
                                 database='db',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
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


.. code:: python

    ## Use Connection Pool

    import pymysql.cursors

    # connect config
    mysql_settings = dict(
        host='localhost',
        user='user',
        password='passwd',
        database='db',
        cursorclass=pymysql.cursors.DictCursor
    )

    ## Create a ConnectionPool object
    pool = pymysql.pool.ConnectionPool(min_idle=1, max_idle=3, **mysql_settings)

    ## Get a connection object from the connection pool
    # When the connection is used up, it will be automatically put back into the connection pool
    # instead of closing the connection.

    # * If you use the `with` statement, you don’t need to manually call the `close()` method.
    #   When the connection object exits after use, it will be called automatically.
    # * If you do not use the `with` statement, you need to manually call the `close()` method of the
    #   connection after using the connection, or call the `return_connection(connection)` method of
    #   the connection pool to return the connection to the connection pool.

    connection = pool.get_connection()

    with connection:
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


This example will print:

.. code:: python

    {'password': 'very-secret', 'id': 1}


Resources
---------

* DB-API 2.0: https://www.python.org/dev/peps/pep-0249/

* MySQL Reference Manuals: https://dev.mysql.com/doc/

* MySQL client/server protocol:
  https://dev.mysql.com/doc/internals/en/client-server-protocol.html

* "Connector" channel in MySQL Community Slack:
  https://lefred.be/mysql-community-on-slack/

* PyMySQL mailing list: https://groups.google.com/forum/#!forum/pymysql-users

License
-------

PyMySQL is released under the MIT License. See LICENSE for more information.
