import os
import cymysql
import unittest

class PyMySQLTestCase(unittest.TestCase):
    # Edit this to suit your test environment.
    test_passwd = os.environ.get("MYSQL_ROOT_PASSWORD", "")
    databases = [
        {"host":"localhost", "user":"root",
         "passwd":test_passwd, "db":"test_cymysql", "use_unicode": True},
        {"host":"localhost", "user":"root", "passwd":test_passwd, "db":"test_cymysql2"}]

    def setUp(self):
        self.connections = []

        for params in self.databases:
            self.connections.append(cymysql.connect(**params))

    def tearDown(self):
        for connection in self.connections:
            connection.close()

