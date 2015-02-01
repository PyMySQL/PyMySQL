import gc
import os
import json
import pymysql

from .._compat import CPYTHON


try:
    import unittest2 as unittest
except ImportError:
    import unittest
import warnings

class PyMySQLTestCase(unittest.TestCase):
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

    def setUp(self):
        self.connections = []
        for params in self.databases:
            self.connections.append(pymysql.connect(**params))
        self.addCleanup(self._teardown_connections)

    def _teardown_connections(self):
        for connection in self.connections:
            connection.close()

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
            cursor.execute("drop table if exists test")
        cursor.execute("create table test (data varchar(10))")
        cursor.close()
        if cleanup:
            self.addCleanup(self.drop_table, connection, tablename)

    def drop_table(self, connection, tablename):
        cursor = connection.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cursor.execute("drop table if exists %s" % tablename)
        cursor.close()

    def safe_gc_collect(self):
        """Ensure cycles are collected via gc.

        Runs additional times on non-CPython platforms.

        """
        gc.collect()
        if not CPYTHON:
            gc.collect()