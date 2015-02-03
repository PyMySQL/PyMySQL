import gc
import json
import os
import re
import unittest
import warnings

from .._compat import CPYTHON

import tornado_mysql
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test


class PyMySQLTestCase(AsyncTestCase):
    # You can specify your test environment creating a file named
    #  "databases.json" or editing the `databases` variable below.
    fname = os.path.join(os.path.dirname(__file__), "databases.json")
    if os.path.exists(fname):
        with open(fname) as f:
            databases = json.load(f)
    else:
        databases = [
            {"host":"localhost","user":"root",
             "passwd":"","db":"test_pymysql", "use_unicode": True, 'local_infile': True},
            {"host":"localhost","user":"root","passwd":"","db":"test_pymysql2"}]

    @gen.coroutine
    def _connect_all(self):
        for params in self.databases:
            conn = yield tornado_mysql.connect(io_loop=self.io_loop, **params)
            self.connections.append(conn)

    def mysql_server_is(self, conn, version_tuple):
        """Return True if the given connection is on the version given or
        greater.

        e.g.::

            if self.mysql_server_is(conn, (5, 6, 4)):
                # do something for MySQL 5.6.4 and above
        """
        server_version = conn.get_server_info()
        server_version_tuple = tuple(
            (int(dig) if dig is not None else 0)
            for dig in
            re.match(r'(\d+)\.(\d+)\.(\d+)', server_version).group(1, 2, 3)
        )
        return server_version_tuple >= version_tuple

    def setUp(self):
        super(PyMySQLTestCase, self).setUp()
        self.connections = []
        self.io_loop.run_sync(self._connect_all)
        self.addCleanup(self._teardown_connections)

    def _teardown_connections(self):
        for connection in self.connections:
            try:
                connection.close()
            except:
                # IOLoop closed already
                pass

    def safe_create_table(self, connection, tablename, ddl, cleanup=False):
        """create a table.

        Ensures any existing version of that table
        is first dropped.

        Also adds a cleanup rule to drop the table after the test
        completes.

        """

        cursor = connection.cursor()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield cursor.execute("drop table if exists test")
        yield cursor.execute("create table test (data varchar(10))")
        yield cursor.close()
        if cleanup:
            self.addCleanup(self.drop_table, connection, tablename)

    def drop_table(self, connection, tablename):
        @self.io_loop.run_sync
        @gen.coroutine
        def _drop_table_inner():
            cursor = connection.cursor()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                yield cursor.execute("drop table if exists %s" % tablename)
            yield cursor.close()

    def safe_gc_collect(self):
        """Ensure cycles are collected via gc.

        Runs additional times on non-CPython platforms.

        """
        gc.collect()
        if not CPYTHON:
            gc.collect()
