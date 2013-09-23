import pymysql
from pymysql.tests import base


class TestConnection(base.PyMySQLTestCase):
    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5"""
        arg = self.databases[0].copy()
        arg['charset'] = 'utf8mb4'
        conn = pymysql.connect(**arg)


if __name__ == "__main__":
    try:
        import unittest2 as unittest
    except ImportError:
        import unittest
    unittest.main()
