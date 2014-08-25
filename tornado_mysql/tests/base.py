import os
import json
import tornado_mysql
import unittest

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
             "passwd":"","db":"test_pymysql", "use_unicode": True},
            {"host":"localhost","user":"root","passwd":"","db":"test_pymysql2"}]

    @gen.coroutine
    def _connect_all(self):
        for params in self.databases:
            conn = yield tornado_mysql.connect(io_loop=self.io_loop, **params)
            self.connections.append(conn)

    def setUp(self):
        super(PyMySQLTestCase, self).setUp()
        self.connections = []
        self.io_loop.run_sync(self._connect_all)

    def tearDown(self):
        for connection in self.connections:
            connection.close()
        super(PyMySQLTestCase, self).tearDown()
