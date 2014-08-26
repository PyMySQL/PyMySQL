==============
Tornado-MySQL
==============

.. image:: https://travis-ci.org/PyMySQL/Tornado-MySQL.svg?branch=tornado
   :target: https://travis-ci.org/PyMySQL/Tornado-MySQL

.. contents::

This package contains a fork of PyMySQL supporting Tornado.

Example
-------

example
~~~~~~~

::

    #!/usr/bin/env python
    from __future__ import print_function

    from tornado import ioloop, gen
    import tornado_mysql

    @gen.coroutine
    def main():
        conn = yield tornado_mysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='mysql')
        cur = conn.cursor()
        yield cur.execute("SELECT Host,User FROM user")
        print(cur.description)
        for row in cur:
           print(row)
        cur.close()
        conn.close()

    ioloop.IOLoop.current().run_sync(main)

example_pool
~~~~~~~~~~~~

::

    #!/usr/bin/env python
    from __future__ import print_function

    from tornado import ioloop, gen
    from tornado_mysql import pools


    pools.DEBUG = True


    POOL = pools.Pool(
        dict(host='127.0.0.1', port=3306, user='root', passwd='', db='mysql'),
        max_idle_connections=1,
        max_recycle_sec=3)


    @gen.coroutine
    def worker(n):
        for i in range(10):
            t = 1
            print(n, "sleeping", t, "seconds")
            cur = yield POOL.execute("SELECT SLEEP(%s)", (t,))
            print(n, cur.fetchall())


    @gen.coroutine
    def main():
        workers = [worker(i) for i in range(10)]
        yield workers


    ioloop.IOLoop.current().run_sync(main)
    print(POOL._opened_conns)


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

Test Suite
----------

If you would like to run the test suite, first copy the file
``.travis.databases.json`` to ``tornado_mysql/tests/databases.json`` and edit the new
file to match your MySQL configuration::

    $ cp .travis.databases.json tornado_mysql/tests/databases.json
    $ $EDITOR tornado_mysql/tests/databases.json

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
