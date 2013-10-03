import os
import json
import pymysql
try:
    import unittest2 as unittest
except ImportError:
    import unittest

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
             "passwd":"","db":"test_pymysql", "use_unicode": True},
            {"host":"localhost","user":"root","passwd":"","db":"test_pymysql2"}]

    def setUp(self):
        self.connections = []
        for params in self.databases:
            self.connections.append(pymysql.connect(**params))

    def tearDown(self):
        for connection in self.connections:
            connection.close()
